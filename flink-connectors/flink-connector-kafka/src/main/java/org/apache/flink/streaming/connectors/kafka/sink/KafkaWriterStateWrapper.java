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

import org.apache.kafka.clients.producer.ProducerConfig;

import javax.annotation.Nullable;

import java.util.Properties;

import static java.util.Objects.requireNonNull;

/**
 * A wrapper around {@link KafkaWriterState} to handle the initialization of the {@link
 * FlinkKafkaInternalProducer}.
 */
class KafkaWriterStateWrapper {

    private static final int DEFAULT_PRODUCER_ID = -1;
    private static final short DEFAULT_EPOCH = (short) -1;

    private final KafkaWriterState state;
    @Nullable private FlinkKafkaInternalProducer<byte[], byte[]> producer;

    public KafkaWriterStateWrapper(
            Properties kafkaProducerConfig, FlinkKafkaInternalProducer<byte[], byte[]> producer) {
        this.producer = producer;
        if (kafkaProducerConfig.containsKey(ProducerConfig.TRANSACTIONAL_ID_CONFIG)) {
            this.state =
                    new KafkaWriterState(
                            kafkaProducerConfig, producer.getProducerId(), producer.getEpoch());
            return;
        }
        this.state = new KafkaWriterState(kafkaProducerConfig, DEFAULT_PRODUCER_ID, DEFAULT_EPOCH);
    }

    public KafkaWriterStateWrapper(KafkaWriterState state) {
        this.state = requireNonNull(state, "kafkaWriterState");
    }

    public KafkaWriterState getState() {
        return state;
    }

    public FlinkKafkaInternalProducer<byte[], byte[]> getProducer() {
        if (producer == null) {
            producer = new FlinkKafkaInternalProducer<>(state.getKafkaProducerConfig());
            // In case, the sink was previously executed with at-least-once guarantee and is now
            // exactly-once.
            if (state.getKafkaProducerConfig().containsKey(ProducerConfig.TRANSACTIONAL_ID_CONFIG)
                    && state.getEpoch() != DEFAULT_EPOCH
                    && state.getProducerId() != DEFAULT_PRODUCER_ID) {
                producer.resumeTransaction(state.getProducerId(), state.getEpoch());
            }
        }
        return producer;
    }
}
