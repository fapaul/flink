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

import java.util.Objects;
import java.util.Properties;

/**
 * This class holds the necessary information to construct a new {@link FlinkKafkaInternalProducer}
 * to commit transactions in {@link KafkaCommitter}.
 */
class KafkaCommittable {

    private final long producerId;
    private final short epoch;
    private final Properties kafkaProducerConfig;

    public KafkaCommittable(FlinkKafkaInternalProducer<byte[], byte[]> producer) {
        this(producer.getProducerId(), producer.getEpoch(), producer.getKafkaProducerConfig());
    }

    public KafkaCommittable(long producerId, short epoch, Properties kafkaProducerConfig) {
        this.producerId = producerId;
        this.epoch = epoch;
        this.kafkaProducerConfig = kafkaProducerConfig;
    }

    public long getProducerId() {
        return producerId;
    }

    public short getEpoch() {
        return epoch;
    }

    public Properties getKafkaProducerConfig() {
        return kafkaProducerConfig;
    }

    @Override
    public String toString() {
        return "KafkaCommittable{"
                + "producerId="
                + producerId
                + ", epoch="
                + epoch
                + ", kafkaProducerConfig="
                + kafkaProducerConfig
                + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        KafkaCommittable that = (KafkaCommittable) o;
        return producerId == that.producerId
                && epoch == that.epoch
                && kafkaProducerConfig.equals(that.kafkaProducerConfig);
    }

    @Override
    public int hashCode() {
        return Objects.hash(producerId, epoch, kafkaProducerConfig);
    }
}
