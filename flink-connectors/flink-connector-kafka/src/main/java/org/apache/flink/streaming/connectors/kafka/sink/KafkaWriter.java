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

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.connectors.kafka.internals.metrics.KafkaMetricMutableWrapper;

import org.apache.flink.shaded.guava18.com.google.common.collect.ImmutableList;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

/**
 * This class is responsible to write records in a Kafka topic and to handle the different delivery
 * {@link DeliveryGuarantee}s.
 *
 * @param <IN> The type of the input elements.
 */
public class KafkaWriter<IN> implements SinkWriter<IN, KafkaCommittable, KafkaWriterState> {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaWriter.class);
    private static final String KEY_DISABLE_METRICS = "flink.disable-metrics";
    private static final String KAFKA_PRODUCER_METRIC_NAME = "KafkaProducer";

    private final DeliveryGuarantee deliveryGuarantee;
    private final Properties kafkaProducerConfig;
    private final String transactionalIdPrefix;
    private final KafkaRecordSerializationSchema<IN> recordSerializer;
    private final Callback deliveryCallback;
    private final AtomicLong pendingRecords = new AtomicLong();
    private final KafkaRecordSerializationSchema.KafkaSinkContext kafkaSinkContext;
    private final List<FlinkKafkaInternalProducer<byte[], byte[]>> producers = new ArrayList<>();
    private final FlinkKafkaInternalProducer<?, ?> cachedProducer;
    private final Map<String, KafkaMetricMutableWrapper> previouslyCreatedMetrics = new HashMap<>();
    private final MetricGroup metricGroup;

    private transient FlinkKafkaInternalProducer<byte[], byte[]> currentProducer;
    private transient KafkaWriterState kafkaWriterState;
    @Nullable private transient volatile Exception producerAsyncException;

    /**
     * Constructor creating a kafka writer.
     *
     * <p>It will throw a {@link RuntimeException} if {@link
     * KafkaRecordSerializationSchema#open(SerializationSchema.InitializationContext)} fails.
     *
     * @param deliveryGuarantee the Sink's delivery guarantee
     * @param kafkaProducerConfig the properties to configure the {@link FlinkKafkaInternalProducer}
     * @param sinkInitContext context to provide information about the runtime environment
     * @param recordSerializer serialize to transform the incoming records to {@link ProducerRecord}
     * @param schemaContext context used to initialize the {@link KafkaRecordSerializationSchema}
     * @param recoveredStates state from an previous execution which need to be covered
     */
    KafkaWriter(
            DeliveryGuarantee deliveryGuarantee,
            Properties kafkaProducerConfig,
            String transactionalIdPrefix,
            Sink.InitContext sinkInitContext,
            KafkaRecordSerializationSchema<IN> recordSerializer,
            SerializationSchema.InitializationContext schemaContext,
            List<KafkaWriterState> recoveredStates) {
        this.deliveryGuarantee = requireNonNull(deliveryGuarantee, "deliveryGuarantee");
        this.kafkaProducerConfig = requireNonNull(kafkaProducerConfig, "kafkaProducerConfig");
        this.transactionalIdPrefix = requireNonNull(transactionalIdPrefix, "transactionalIdPrefix");
        this.recordSerializer = requireNonNull(recordSerializer, "recordSerializer");
        try {
            recordSerializer.open(schemaContext);
        } catch (Exception e) {
            throw new RuntimeException("Cannot initialize schema.", e);
        }
        this.deliveryCallback =
                (metadata, exception) -> {
                    if (exception != null && producerAsyncException == null) {
                        producerAsyncException = exception;
                    }
                    acknowledgeMessage();
                };
        this.cachedProducer = new FlinkKafkaInternalProducer<>(kafkaProducerConfig);
        this.metricGroup = sinkInitContext.metricGroup();
        this.kafkaSinkContext =
                new DefaultKafkaSinkContext(
                        requireNonNull(sinkInitContext, "sinkInitContext"), cachedProducer);
        this.kafkaWriterState =
                recoverAndInitializeState(requireNonNull(recoveredStates, "recoveredStates"));
        this.currentProducer = beginTransaction();
        producers.add(currentProducer);
    }

    @Override
    public void write(IN element, Context context) throws IOException {
        checkErroneous();
        final ProducerRecord<byte[], byte[]> record =
                recordSerializer.serialize(element, kafkaSinkContext, context.timestamp());
        pendingRecords.incrementAndGet();
        currentProducer.send(record, deliveryCallback);
    }

    @Override
    public List<KafkaCommittable> prepareCommit(boolean flush) throws IOException {
        flushRecords(flush);
        currentProducer = beginTransaction();
        final List<KafkaCommittable> committables = commit();
        producers.add(currentProducer);
        return committables;
    }

    @Override
    public List<KafkaWriterState> snapshotState() throws IOException {
        return ImmutableList.of(kafkaWriterState);
    }

    @Override
    public void close() throws Exception {
        cachedProducer.close(Duration.ZERO);
        currentProducer.close(Duration.ZERO);
    }

    private KafkaWriterState recoverAndInitializeState(List<KafkaWriterState> recoveredStates) {
        final int subtaskId = kafkaSinkContext.getParallelInstanceId();
        if (recoveredStates.isEmpty()) {
            return new KafkaWriterState(subtaskId, subtaskId + 1, transactionalIdPrefix);
        }
        final Map<Integer, KafkaWriterState> taskOffsetMapping =
                recoveredStates.stream()
                        .collect(
                                Collectors.toMap(
                                        KafkaWriterState::getSubtaskId, Function.identity()));
        abortTransactions(recoveredStates);
        if (!taskOffsetMapping.containsKey(subtaskId)) {
            throw new IllegalStateException(
                    "It is expected that state from previous executions is distributed to the same subtask id.");
        }
        final KafkaWriterState lastState = taskOffsetMapping.get(subtaskId);
        if (!lastState.getTransactionalIdPrefix().equals(transactionalIdPrefix)) {
            LOG.info(
                    "Transactional id prefix from previous execution {} has changed to {}.",
                    lastState.getTransactionalIdPrefix(),
                    transactionalIdPrefix);
            return new KafkaWriterState(
                    subtaskId, lastState.getSubtaskId() + 1, transactionalIdPrefix);
        }
        return new KafkaWriterState(
                subtaskId,
                taskOffsetMapping.get(subtaskId).getTransactionalIdOffset(),
                transactionalIdPrefix);
    }

    private void abortTransactions(List<KafkaWriterState> recoveredStates) {
        recoveredStates.forEach(
                state -> {
                    // don't mess with the original configuration or any other
                    // properties of the
                    // original object
                    // -> create an internal kafka producer on our own and do not rely
                    // on
                    //    initTransactionalProducer().
                    final Properties myConfig = new Properties();
                    myConfig.putAll(kafkaProducerConfig);
                    initTransactionalProducerConfig(
                            myConfig,
                            state.getTransactionalIdOffset(),
                            state.getTransactionalIdPrefix(),
                            state.getSubtaskId());
                    LOG.info(
                            "Aborting Kafka transaction {}.",
                            myConfig.get(ProducerConfig.TRANSACTIONAL_ID_CONFIG));
                    FlinkKafkaInternalProducer<byte[], byte[]> kafkaProducer = null;
                    try {
                        kafkaProducer = new FlinkKafkaInternalProducer<>(myConfig);
                        // it suffices to call initTransactions - this will abort any
                        // lingering transactions
                        kafkaProducer.initTransactions();
                    } finally {
                        if (kafkaProducer != null) {
                            kafkaProducer.close(Duration.ofSeconds(0));
                        }
                    }
                });
    }

    private void acknowledgeMessage() {
        pendingRecords.decrementAndGet();
    }

    private void checkErroneous() {
        Exception e = producerAsyncException;
        if (e != null) {
            // prevent double throwing
            producerAsyncException = null;
            throw new RuntimeException("Failed to send data to Kafka: " + e.getMessage(), e);
        }
    }

    private FlinkKafkaInternalProducer<byte[], byte[]> beginTransaction() {
        switch (deliveryGuarantee) {
            case EXACTLY_ONCE:
                if (currentProducer != null) {
                    currentProducer.close(Duration.ZERO);
                }
                final FlinkKafkaInternalProducer<byte[], byte[]> transactionalProducer =
                        createTransactionalProducer();
                initMetrics(transactionalProducer);
                transactionalProducer.beginTransaction();
                return transactionalProducer;
            case AT_LEAST_ONCE:
            case NONE:
                if (currentProducer == null) {
                    final FlinkKafkaInternalProducer<byte[], byte[]> producer =
                            new FlinkKafkaInternalProducer<>(kafkaProducerConfig);
                    initMetrics(producer);
                    return producer;
                }
                LOG.debug("Reusing existing KafkaProducer");
                return currentProducer;
            default:
                throw new UnsupportedOperationException(
                        "Unsupported Kafka writer semantic " + deliveryGuarantee);
        }
    }

    private void flushRecords(boolean finalFlush) {
        switch (deliveryGuarantee) {
            case EXACTLY_ONCE:
            case AT_LEAST_ONCE:
                currentProducer.flush();
                final long pendingRecordsCount = pendingRecords.get();
                if (pendingRecordsCount != 0) {
                    throw new IllegalStateException(
                            "Pending record count must be zero at this point: "
                                    + pendingRecordsCount);
                }
                break;
            case NONE:
                if (finalFlush) {
                    currentProducer.flush();
                }
                break;
            default:
                throw new UnsupportedOperationException(
                        "Unsupported Kafka writer semantic " + deliveryGuarantee);
        }
        // if the flushed requests has errors, we should propagate it also and fail the checkpoint
        checkErroneous();
    }

    private List<KafkaCommittable> commit() {
        final List<KafkaCommittable> committables;
        switch (deliveryGuarantee) {
            case EXACTLY_ONCE:
                committables =
                        producers.stream().map(KafkaCommittable::new).collect(Collectors.toList());
                producers.clear();
                break;
            case AT_LEAST_ONCE:
            case NONE:
                committables = new ArrayList<>();
                break;
            default:
                throw new UnsupportedOperationException(
                        "Unsupported Kafka writer semantic " + deliveryGuarantee);
        }
        LOG.info("Committing {} committables.", committables);
        return committables;
    }

    /**
     * For each checkpoint we create new {@link FlinkKafkaInternalProducer} so that new transactions
     * will not clash with transactions created during previous checkpoints ({@code
     * producer.initTransactions()} assures that we obtain new producerId and epoch counters).
     */
    private FlinkKafkaInternalProducer<byte[], byte[]> createTransactionalProducer() {
        final int parallelism = kafkaSinkContext.getNumberOfParallelInstances();
        final long transactionalIdOffset =
                kafkaWriterState.getTransactionalIdOffset() + parallelism;
        final Properties copiedProducerConfig = new Properties();
        copiedProducerConfig.putAll(kafkaProducerConfig);
        initTransactionalProducerConfig(
                copiedProducerConfig,
                transactionalIdOffset,
                transactionalIdPrefix,
                kafkaSinkContext.getParallelInstanceId());
        final FlinkKafkaInternalProducer<byte[], byte[]> producer =
                new FlinkKafkaInternalProducer<>(copiedProducerConfig);
        producer.initTransactions();
        kafkaWriterState =
                new KafkaWriterState(
                        kafkaSinkContext.getParallelInstanceId(),
                        transactionalIdOffset,
                        transactionalIdPrefix);
        LOG.debug(
                "Created new transactional producer {}",
                copiedProducerConfig.get(ProducerConfig.TRANSACTIONAL_ID_CONFIG));
        return producer;
    }

    private static String buildTransactionalId(
            long offset, String transactionalIdPrefix, int subtaskId) {
        final StringBuilder sb = new StringBuilder();
        return sb.append(transactionalIdPrefix)
                .append("-")
                .append(subtaskId)
                .append("-")
                .append(offset)
                .toString();
    }

    private static void initTransactionalProducerConfig(
            Properties producerConfig,
            long transactionalIdOffset,
            String transactionalIdPrefix,
            int subtaskId) {
        producerConfig.put(
                ProducerConfig.TRANSACTIONAL_ID_CONFIG,
                buildTransactionalId(transactionalIdOffset, transactionalIdPrefix, subtaskId));
    }

    private void initMetrics(FlinkKafkaInternalProducer<byte[], byte[]> producer) {
        if (producer.getKafkaProducerConfig().containsKey(KEY_DISABLE_METRICS)
                && Boolean.parseBoolean(
                        producer.getKafkaProducerConfig().get(KEY_DISABLE_METRICS).toString())) {
            return;
        }
        final MetricGroup kafkaMetricGroup = metricGroup.addGroup(KAFKA_PRODUCER_METRIC_NAME);
        producer.metrics().entrySet().forEach(initMetric(kafkaMetricGroup));
    }

    private Consumer<Map.Entry<MetricName, ? extends Metric>> initMetric(
            MetricGroup kafkaMetricGroup) {
        return (entry) -> {
            final String name = entry.getKey().name();
            final Metric metric = entry.getValue();
            if (previouslyCreatedMetrics.containsKey(name)) {
                final KafkaMetricMutableWrapper wrapper = previouslyCreatedMetrics.get(name);
                wrapper.setKafkaMetric(metric);
            } else {
                final KafkaMetricMutableWrapper wrapper = new KafkaMetricMutableWrapper(metric);
                previouslyCreatedMetrics.put(name, wrapper);
                kafkaMetricGroup.gauge(name, wrapper);
            }
        };
    }
}
