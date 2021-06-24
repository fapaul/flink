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

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;
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

    private final DeliveryGuarantee deliveryGuarantee;
    private final Properties kafkaProducerConfig;
    private final int kafkaProducerPoolSize;
    private final KafkaRecordSerializationSchema<IN> recordSerializer;
    private final Callback deliveryCallback;
    private final AtomicLong pendingRecords = new AtomicLong();
    private final KafkaRecordSerializationSchema.KafkaSinkContext kafkaSinkContext;
    private final List<KafkaWriterStateWrapper> states;
    private final FlinkKafkaInternalProducer<?, ?> cachedProducer;

    private transient KafkaWriterStateWrapper currentState;
    @Nullable private transient volatile Exception producerAsyncException;

    /**
     * Constructor creating a kafka writer.
     *
     * <p>It will throw a {@link RuntimeException} if {@link
     * KafkaRecordSerializationSchema#open(SerializationSchema.InitializationContext)} fails.
     *
     * @param deliveryGuarantee
     * @param kafkaProducerConfig the properties to configure the {@link FlinkKafkaInternalProducer}
     * @param sinkInitContext context to provide information about the runtime environment
     * @param recordSerializer serialize to transform the incoming records to {@link ProducerRecord}
     * @param schemaContext context used to initialize the {@link KafkaRecordSerializationSchema}
     * @param recoveredStates state from an previous execution which need to be covered
     */
    KafkaWriter(
            DeliveryGuarantee deliveryGuarantee,
            Properties kafkaProducerConfig,
            int kafkaProducerPoolSize,
            Sink.InitContext sinkInitContext,
            KafkaRecordSerializationSchema<IN> recordSerializer,
            SerializationSchema.InitializationContext schemaContext,
            List<KafkaWriterState> recoveredStates) {
        this.deliveryGuarantee = requireNonNull(deliveryGuarantee, "deliveryGuarantee");
        this.kafkaProducerConfig = requireNonNull(kafkaProducerConfig, "kafkaProducerConfig");
        this.kafkaProducerPoolSize = kafkaProducerPoolSize;
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
        this.kafkaSinkContext =
                new DefaultKafkaSinkContext(
                        requireNonNull(sinkInitContext, "sinkInitContext"), cachedProducer);
        this.states =
                requireNonNull(recoveredStates, "recoveredStates").stream()
                        .map(KafkaWriterStateWrapper::new)
                        .collect(Collectors.toList());
        this.currentState = beginTransaction();
    }

    @Override
    public void write(IN element, Context context) throws IOException {
        checkErroneous();
        final ProducerRecord<byte[], byte[]> record =
                recordSerializer.serialize(element, kafkaSinkContext, context.timestamp());
        pendingRecords.incrementAndGet();
        currentState.getProducer().send(record, deliveryCallback);
        LOG.info("Sent record");
    }

    @Override
    public List<KafkaCommittable> prepareCommit(boolean flush) throws IOException {
        flushRecords(flush);
        states.add(new KafkaWriterStateWrapper(kafkaProducerConfig, currentState.getProducer()));
        return commit();
    }

    @Override
    public List<KafkaWriterState> snapshotState() throws IOException {
        final Iterator<KafkaWriterStateWrapper> it = states.iterator();
        final List<KafkaWriterState> persistedStates = new ArrayList<>();
        while (it.hasNext()) {
            final KafkaWriterStateWrapper stateWrapper = it.next();
            persistedStates.add(stateWrapper.getState());
            it.remove();
        }
        LOG.info("Committing {} committables.", persistedStates.size());
        return persistedStates;
    }

    @Override
    public void close() throws Exception {
        cachedProducer.close(Duration.ZERO);
        currentState.getProducer().close(Duration.ZERO);
        states.forEach(state -> state.getProducer().close(Duration.ZERO));
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

    private KafkaWriterStateWrapper beginTransaction() {
        switch (deliveryGuarantee) {
            case EXACTLY_ONCE:
                throw new UnsupportedOperationException("No implemented yet.");
            case AT_LEAST_ONCE:
            case NONE:
                if (currentState == null) {
                    return new KafkaWriterStateWrapper(
                            kafkaProducerConfig,
                            new FlinkKafkaInternalProducer<>(kafkaProducerConfig));
                }
                LOG.debug("Reusing existing KafkaProducer");
                return new KafkaWriterStateWrapper(kafkaProducerConfig, currentState.getProducer());
            default:
                throw new UnsupportedOperationException(
                        "Unsupported Kafka writer semantic " + deliveryGuarantee);
        }
    }

    private void flushRecords(boolean finalFlush) {
        switch (deliveryGuarantee) {
            case EXACTLY_ONCE:
            case AT_LEAST_ONCE:
                currentState.getProducer().flush();
                final long pendingRecordsCount = pendingRecords.get();
                if (pendingRecordsCount != 0) {
                    throw new IllegalStateException(
                            "Pending record count must be zero at this point: "
                                    + pendingRecordsCount);
                }
                break;
            case NONE:
                if (finalFlush) {
                    currentState.getProducer().flush();
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
                        states.stream().map(KafkaCommittable::new).collect(Collectors.toList());
                break;
            case AT_LEAST_ONCE:
            case NONE:
                committables = new ArrayList<>();
                break;
            default:
                throw new UnsupportedOperationException(
                        "Unsupported Kafka writer semantic " + deliveryGuarantee);
        }
        currentState = beginTransaction();
        return committables;
    }
}
