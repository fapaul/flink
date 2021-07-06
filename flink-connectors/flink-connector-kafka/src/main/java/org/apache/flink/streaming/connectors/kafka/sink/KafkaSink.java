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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.connector.sink.Committer;
import org.apache.flink.api.connector.sink.GlobalCommitter;
import org.apache.flink.api.connector.sink.InitContextInitializationContextAdapter;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.api.java.ClosureCleaner;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

import static java.util.Objects.requireNonNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * TODO: write once exactly-once is implemented.
 *
 * @param <IN>
 */
class KafkaSink<IN> implements Sink<IN, KafkaCommittable, KafkaWriterState, Void> {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaSink.class);
    private static final Duration DEFAULT_KAFKA_TRANSACTION_TIMEOUT = Duration.ofHours(1);

    private final DeliveryGuarantee deliveryGuarantee;
    private final KafkaRecordSerializationSchema<IN> recordSerializer;
    private final Properties kafkaProducerConfig;
    private final int kafkaProducerPoolSize;

    private KafkaSink(
            DeliveryGuarantee deliveryGuarantee,
            Properties kafkaProducerConfig,
            int kafkaProducerPoolSize,
            KafkaRecordSerializationSchema<IN> recordSerializer) {
        this.deliveryGuarantee = deliveryGuarantee;
        this.kafkaProducerPoolSize = kafkaProducerPoolSize;
        this.kafkaProducerConfig = kafkaProducerConfig;
        this.recordSerializer = recordSerializer;
    }

    @Override
    public SinkWriter<IN, KafkaCommittable, KafkaWriterState> createWriter(
            InitContext context, List<KafkaWriterState> states) throws IOException {
        return new KafkaWriter<>(
                deliveryGuarantee,
                kafkaProducerConfig,
                kafkaProducerPoolSize,
                context,
                recordSerializer,
                new InitContextInitializationContextAdapter(
                        context, metricGroup -> metricGroup.addGroup("user")),
                states);
    }

    @Override
    public Optional<Committer<KafkaCommittable>> createCommitter() throws IOException {
        return Optional.empty();
    }

    @Override
    public Optional<GlobalCommitter<KafkaCommittable, Void>> createGlobalCommitter()
            throws IOException {
        return Optional.empty();
    }

    @Override
    public Optional<SimpleVersionedSerializer<KafkaCommittable>> getCommittableSerializer() {
        return Optional.empty();
    }

    @Override
    public Optional<SimpleVersionedSerializer<Void>> getGlobalCommittableSerializer() {
        return Optional.empty();
    }

    @Override
    public Optional<SimpleVersionedSerializer<KafkaWriterState>> getWriterStateSerializer() {
        return Optional.of(new KafkaWriterStateSerializer(kafkaProducerConfig));
    }

    /**
     * TODO: write once exactly-once is implemented.
     *
     * @param <IN>
     */
    public static class Builder<IN> {

        private DeliveryGuarantee deliveryGuarantee = DeliveryGuarantee.NONE;
        private int kafkaProducersPoolSize = 5;

        private Properties kafkaProducerConfig;
        private KafkaRecordSerializationSchema<IN> recordSerializer;

        public Builder<IN> setTopic(String topic) {
            return this;
        }

        public Builder<IN> setSemantic(DeliveryGuarantee deliveryGuarantee) {
            this.deliveryGuarantee = requireNonNull(deliveryGuarantee, "semantic");
            return this;
        }

        public Builder<IN> setKafkaProducerConfig(Properties kafkaProducerConfig) {
            this.kafkaProducerConfig = requireNonNull(kafkaProducerConfig, "kafkaProducerConfig");
            // set the producer configuration properties for kafka record key value serializers.
            if (!kafkaProducerConfig.containsKey(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG)) {
                kafkaProducerConfig.put(
                        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                        ByteArraySerializer.class.getName());
            } else {
                LOG.warn(
                        "Overwriting the '{}' is not recommended",
                        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG);
            }

            if (!kafkaProducerConfig.containsKey(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG)) {
                kafkaProducerConfig.put(
                        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                        ByteArraySerializer.class.getName());
            } else {
                LOG.warn(
                        "Overwriting the '{}' is not recommended",
                        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG);
            }

            if (!kafkaProducerConfig.containsKey(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG)) {
                final long timeout = DEFAULT_KAFKA_TRANSACTION_TIMEOUT.toMillis();
                checkState(
                        timeout < Integer.MAX_VALUE && timeout > 0,
                        "timeout does not fit into 32 bit integer");
                kafkaProducerConfig.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, (int) timeout);
                LOG.warn(
                        "Property [{}] not specified. Setting it to {}",
                        ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,
                        DEFAULT_KAFKA_TRANSACTION_TIMEOUT);
            }
            return this;
        }

        public Builder<IN> setRecordSerializer(
                KafkaRecordSerializationSchema<IN> recordSerializer) {
            this.recordSerializer = recordSerializer;
            ClosureCleaner.clean(
                    this.recordSerializer, ExecutionConfig.ClosureCleanerLevel.RECURSIVE, true);
            return this;
        }

        public Builder<IN> setKafkaProducersPoolSize(int kafkaProducersPoolSize) {
            this.kafkaProducersPoolSize = kafkaProducersPoolSize;
            return this;
        }

        public KafkaSink<IN> build() {
            requireNonNull(kafkaProducerConfig, "kafkaProducerConfig");
            checkState(kafkaProducersPoolSize > 0, "kafkaProducersPoolSize must be non empty");
            checkState(
                    kafkaProducerConfig.containsKey(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG),
                    "kafkaProducerConfig must contain " + ProducerConfig.BOOTSTRAP_SERVERS_CONFIG);
            return new KafkaSink<>(
                    deliveryGuarantee,
                    kafkaProducerConfig,
                    kafkaProducersPoolSize,
                    requireNonNull(recordSerializer, "recordSerializer"));
        }
    }
}
