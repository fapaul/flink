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
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.TestLogger;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
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
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
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

    @ClassRule
    public static final KafkaContainer KAFKA_CONTAINER =
            new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:5.5.2"))
                    .withEmbeddedZookeeper()
                    .withNetwork(NETWORK)
                    .withLogConsumer(LOG_CONSUMER)
                    .withNetworkAliases(INTER_CONTAINER_KAFKA_ALIAS);

    @Before
    public void setUp() throws ExecutionException, InterruptedException, TimeoutException {
        topic = UUID.randomUUID().toString();
        createTestTopic(topic, 1, TOPIC_REPLICATION_FACTOR);
    }

    @After
    public void tearDown() throws ExecutionException, InterruptedException, TimeoutException {
        deleteTestTopic(topic);
    }

    @Test
    public void testWriteRecordsToKafkaWithAtLeastOnceGuarantee() throws Exception {
        writeRecordsToKafka(DeliveryGuarantee.AT_LEAST_ONCE);
    }

    @Test
    public void testWriteRecordsToKafkaWithNoneGuarantee() throws Exception {
        writeRecordsToKafka(DeliveryGuarantee.NONE);
    }

    @Test
    public void testRecoveryWithAtLeastOnceGuarantee() throws Exception {
        final StreamExecutionEnvironment env = new LocalStreamEnvironment();
        env.enableCheckpointing(300L);
        DataStreamSource<Long> source = env.fromSequence(1, 10);
        DataStream<Long> stream = source.map(new FailingCheckpointMapper());

        stream.sinkTo(
                new KafkaSink.Builder<Long>()
                        .setKafkaProducerConfig(getKafkaClientConfiguration())
                        .setSemantic(DeliveryGuarantee.AT_LEAST_ONCE)
                        .setRecordSerializer(new RecordSerializer(topic))
                        .build());
        env.execute();

        final List<ConsumerRecord<byte[], byte[]>> collectedRecords =
                drainAllRecordsFromTopic(topic);
        assertThat(
                serializeValues(collectedRecords),
                hasItems(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L));
        checkProducerLeak();
    }

    private void writeRecordsToKafka(DeliveryGuarantee deliveryGuarantee) throws Exception {
        final StreamExecutionEnvironment env = new LocalStreamEnvironment();
        env.enableCheckpointing(1000L);
        DataStreamSource<Long> source = env.fromSequence(1, 10);
        source.sinkTo(
                new KafkaSink.Builder<Long>()
                        .setKafkaProducerConfig(getKafkaClientConfiguration())
                        .setSemantic(deliveryGuarantee)
                        .setRecordSerializer(new RecordSerializer(topic))
                        .build());
        env.execute();

        final List<ConsumerRecord<byte[], byte[]>> collectedRecords =
                drainAllRecordsFromTopic(topic);
        assertEquals(collectedRecords.size(), 10);
        assertThat(
                serializeValues(collectedRecords),
                contains(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L));
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
        final Properties consumerConfig = getKafkaClientConfiguration();
        consumerConfig.put("key.deserializer", ByteArrayDeserializer.class.getName());
        consumerConfig.put("value.deserializer", ByteArrayDeserializer.class.getName());
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

        private long lastCheckpointId = 0;
        private int emittedBetweenCheckpoint = 0;

        @Override
        public Long map(Long value) throws Exception {
            if (lastCheckpointId == 1 && emittedBetweenCheckpoint > 0) {
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
}
