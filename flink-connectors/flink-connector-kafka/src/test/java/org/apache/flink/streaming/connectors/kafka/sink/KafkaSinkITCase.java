/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.kafka.sink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.testutils.junit.SharedObjects;
import org.apache.flink.testutils.junit.SharedReference;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.guava18.com.google.common.collect.ImmutableList;
import org.apache.flink.shaded.guava18.com.google.common.collect.ImmutableMap;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.DockerImageName;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/** Tests for using KafkaSink writing to a Kafka cluster. */
public class KafkaSinkITCase extends TestLogger {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaSinkITCase.class);
    private static final Slf4jLogConsumer LOG_CONSUMER = new Slf4jLogConsumer(LOG);
    private static final String INTER_CONTAINER_KAFKA_ALIAS = "kafka";
    private static final Network NETWORK = Network.newNetwork();
    private static final int ZK_TIMEOUT_MILLIS = 30000;
    private static final short TOPIC_REPLICATION_FACTOR = 1;
    private static final Duration CONSUMER_POLL_DURATION = Duration.ofSeconds(1);

    private String topic;
    private SharedReference<AtomicLong> emittedRecordsCount;
    private SharedReference<AtomicLong> emittedRecordsWithCheckpoint;
    private SharedReference<AtomicBoolean> failed;

    @ClassRule
    public static final KafkaContainer KAFKA_CONTAINER =
            new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:5.5.2"))
                    .withEmbeddedZookeeper()
                    .withEnv(
                            ImmutableMap.of(
                                    "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR",
                                    "1",
                                    "KAFKA_TRANSACTION_MAX_TIMEOUT_MS",
                                    String.valueOf(Duration.ofHours(2).toMillis()),
                                    "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR",
                                    "1",
                                    "KAFKA_MIN_INSYNC_REPLICAS",
                                    "1"))
                    .withNetwork(NETWORK)
                    .withLogConsumer(LOG_CONSUMER)
                    .withNetworkAliases(INTER_CONTAINER_KAFKA_ALIAS);

    @Rule public final SharedObjects sharedObjects = SharedObjects.create();

    @Before
    public void setUp() throws ExecutionException, InterruptedException, TimeoutException {
        emittedRecordsCount = sharedObjects.add(new AtomicLong());
        emittedRecordsWithCheckpoint = sharedObjects.add(new AtomicLong());
        failed = sharedObjects.add(new AtomicBoolean(false));
        topic = UUID.randomUUID().toString();
        createTestTopic(topic, 1, TOPIC_REPLICATION_FACTOR);
    }

    @After
    public void tearDown() throws ExecutionException, InterruptedException, TimeoutException {
        deleteTestTopic(topic);
    }

    @Test
    public void testWriteRecordsToKafkaWithAtLeastOnceGuarantee() throws Exception {
        writeRecordsToKafka(DeliveryGuarantee.AT_LEAST_ONCE, emittedRecordsCount);
    }

    @Test
    public void testWriteRecordsToKafkaWithNoneGuarantee() throws Exception {
        writeRecordsToKafka(DeliveryGuarantee.NONE, emittedRecordsCount);
    }

    @Test
    public void testWriteRecordsToKafkaWithExactlyOnceGuarantee() throws Exception {
        writeRecordsToKafka(DeliveryGuarantee.EXACTLY_ONCE, emittedRecordsWithCheckpoint);
    }

    @Test
    public void testRecoveryWithAtLeastOnceGuarantee() throws Exception {
        testRecoveryWithAssertion(
                DeliveryGuarantee.AT_LEAST_ONCE,
                (records) ->
                        assertThat(records, hasItems(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L)));
    }

    @Test
    public void testRecoveryWithAtExactlyOnceGuarantee() throws Exception {
        testRecoveryWithAssertion(
                DeliveryGuarantee.EXACTLY_ONCE,
                (records) ->
                        assertEquals(
                                records,
                                ImmutableList.of(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L)));
    }

    private void testRecoveryWithAssertion(
            DeliveryGuarantee guarantee, java.util.function.Consumer<List<Long>> recordsAssertion)
            throws Exception {
        final StreamExecutionEnvironment env = new LocalStreamEnvironment();
        env.enableCheckpointing(300L);
        DataStreamSource<Long> source = env.fromSequence(1, 10);
        DataStream<Long> stream = source.map(new FailingCheckpointMapper(failed));

        stream.sinkTo(
                new KafkaSink.Builder<Long>()
                        .setKafkaProducerConfig(getKafkaClientConfiguration())
                        .setSemantic(guarantee)
                        .setRecordSerializer(new RecordSerializer(topic))
                        .build());
        env.execute();

        final List<ConsumerRecord<byte[], byte[]>> collectedRecords =
                drainAllRecordsFromTopic(topic);
        recordsAssertion.accept(serializeValues(collectedRecords));
        checkProducerLeak();
    }

    private void writeRecordsToKafka(
            DeliveryGuarantee deliveryGuarantee, SharedReference<AtomicLong> expectedRecords)
            throws Exception {
        final StreamExecutionEnvironment env = new LocalStreamEnvironment();
        env.enableCheckpointing(1000L);
        final DataStream<Long> source =
                env.addSource(
                        new InfiniteIntegerSource(
                                emittedRecordsCount, emittedRecordsWithCheckpoint));
        source.sinkTo(
                new KafkaSink.Builder<Long>()
                        .setKafkaProducerConfig(getKafkaClientConfiguration())
                        .setSemantic(deliveryGuarantee)
                        .setRecordSerializer(new RecordSerializer(topic))
                        .build());
        env.execute();

        final List<ConsumerRecord<byte[], byte[]>> collectedRecords =
                drainAllRecordsFromTopic(topic);
        final long recordsCount = expectedRecords.get().get();
        assertEquals(collectedRecords.size(), recordsCount);
        assertEquals(
                serializeValues(collectedRecords),
                LongStream.range(0, recordsCount).boxed().collect(Collectors.toList()));
        checkProducerLeak();
    }

    private static List<Long> serializeValues(List<ConsumerRecord<byte[], byte[]>> records) {
        return records.stream()
                .map(
                        record -> {
                            final ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
                            final byte[] value = record.value();
                            buffer.put(value, 0, value.length);
                            buffer.flip();
                            return buffer.getLong();
                        })
                .collect(Collectors.toList());
    }

    private static Properties getKafkaClientConfiguration() {
        final Properties standardProps = new Properties();
        standardProps.put("bootstrap.servers", KAFKA_CONTAINER.getBootstrapServers());
        standardProps.put("group.id", "flink-tests");
        standardProps.put("enable.auto.commit", false);
        standardProps.put("auto.offset.reset", "earliest");
        standardProps.put("max.partition.fetch.bytes", 256);
        standardProps.put("zookeeper.session.timeout.ms", ZK_TIMEOUT_MILLIS);
        standardProps.put("zookeeper.connection.timeout.ms", ZK_TIMEOUT_MILLIS);
        return standardProps;
    }

    private Consumer<byte[], byte[]> createTestConsumer(String topic) {
        final Properties consumerConfig = new Properties();
        consumerConfig.putAll(getKafkaClientConfiguration());
        consumerConfig.put("key.deserializer", ByteArrayDeserializer.class.getName());
        consumerConfig.put("value.deserializer", ByteArrayDeserializer.class.getName());
        consumerConfig.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        final KafkaConsumer<byte[], byte[]> kafkaConsumer = new KafkaConsumer<>(consumerConfig);
        kafkaConsumer.subscribe(Collections.singletonList(topic));
        return kafkaConsumer;
    }

    private void createTestTopic(String topic, int numPartitions, short replicationFactor)
            throws ExecutionException, InterruptedException, TimeoutException {
        Map<String, Object> properties = new HashMap<>();
        properties.put(
                CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
                KAFKA_CONTAINER.getBootstrapServers());
        try (AdminClient admin = AdminClient.create(properties)) {
            final CreateTopicsResult result =
                    admin.createTopics(
                            Collections.singletonList(
                                    new NewTopic(topic, numPartitions, replicationFactor)));
            result.all().get(1, TimeUnit.MINUTES);
        }
    }

    private void deleteTestTopic(String topic)
            throws ExecutionException, InterruptedException, TimeoutException {
        Map<String, Object> properties = new HashMap<>();
        properties.put(
                CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
                KAFKA_CONTAINER.getBootstrapServers());
        try (AdminClient admin = AdminClient.create(properties)) {
            final DeleteTopicsResult result = admin.deleteTopics(Collections.singletonList(topic));
            result.all().get(1, TimeUnit.MINUTES);
        }
    }

    private List<ConsumerRecord<byte[], byte[]>> drainAllRecordsFromTopic(String topic) {
        final List<ConsumerRecord<byte[], byte[]>> collectedRecords = new ArrayList<>();
        try (Consumer<byte[], byte[]> consumer = createTestConsumer(topic)) {
            ConsumerRecords<byte[], byte[]> records = consumer.poll(CONSUMER_POLL_DURATION);
            // Drain the kafka topic till all records are consumed
            while (!records.isEmpty()) {
                records.records(topic).forEach(collectedRecords::add);
                records = consumer.poll(CONSUMER_POLL_DURATION);
            }
        }
        return collectedRecords;
    }

    private static class RecordSerializer implements KafkaRecordSerializationSchema<Long> {

        private final String topic;

        public RecordSerializer(String topic) {
            this.topic = topic;
        }

        @Override
        public ProducerRecord<byte[], byte[]> serialize(
                Long element, KafkaSinkContext context, Long timestamp) {
            final ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
            buffer.putLong(element);
            return new ProducerRecord<>(topic, 0, null, null, buffer.array());
        }
    }

    /** Fails after a checkpoint is taken and the next record was emitted. */
    private static class FailingCheckpointMapper
            implements MapFunction<Long, Long>, CheckpointListener {

        private final SharedReference<AtomicBoolean> failed;

        private long lastCheckpointId = 0;
        private int emittedBetweenCheckpoint = 0;

        FailingCheckpointMapper(SharedReference<AtomicBoolean> failed) {
            this.failed = failed;
        }

        @Override
        public Long map(Long value) throws Exception {
            if (lastCheckpointId >= 1 && emittedBetweenCheckpoint > 0 && !failed.get().get()) {
                failed.get().set(true);
                throw new RuntimeException("Planned exception.");
            }
            Thread.sleep(50);
            emittedBetweenCheckpoint++;
            return value;
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) throws Exception {
            lastCheckpointId = checkpointId;
            emittedBetweenCheckpoint = 0;
        }
    }

    private void checkProducerLeak() {
        for (Thread t : Thread.getAllStackTraces().keySet()) {
            if (t.getName().contains("kafka-producer-network-thread")) {
                fail("Detected producer leak. Thread name: " + t.getName());
            }
        }
    }

    /**
     * Exposes information about how man records have been emitted overall and at the beginning of a
     * checkpoint.
     */
    private static final class InfiniteIntegerSource
            implements SourceFunction<Long>, CheckpointListener, CheckpointedFunction {

        private final SharedReference<AtomicLong> emittedRecordsCount;
        private final SharedReference<AtomicLong> emittedRecordsWithCheckpoint;

        private volatile boolean running = true;
        private long counter = 0;

        InfiniteIntegerSource(
                SharedReference<AtomicLong> emittedRecordsCount,
                SharedReference<AtomicLong> emittedRecordsWithCheckpoint) {
            this.emittedRecordsCount = emittedRecordsCount;
            this.emittedRecordsWithCheckpoint = emittedRecordsWithCheckpoint;
        }

        @Override
        public void run(SourceContext<Long> ctx) throws Exception {
            while (running) {
                emittedRecordsCount.get().addAndGet(1);
                ctx.collect(counter++);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) throws Exception {
            running = false;
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            emittedRecordsWithCheckpoint.get().set(counter - 1);
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {}
    }
}
