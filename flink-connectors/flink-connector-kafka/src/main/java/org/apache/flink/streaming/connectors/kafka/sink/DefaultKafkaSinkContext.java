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

import org.apache.flink.api.connector.sink.Sink;

import org.apache.kafka.common.PartitionInfo;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Context providing information to assist constructing a {@link
 * org.apache.kafka.clients.producer.ProducerRecord}.
 */
class DefaultKafkaSinkContext implements KafkaRecordSerializationSchema.KafkaSinkContext {

    private final Sink.InitContext context;
    private final FlinkKafkaInternalProducer<?, ?> producer;

    public DefaultKafkaSinkContext(
            Sink.InitContext context, FlinkKafkaInternalProducer<?, ?> producer) {
        this.context = context;
        this.producer = producer;
    }

    @Override
    public int getParallelInstanceId() {
        return context.getSubtaskId();
    }

    @Override
    public int getNumberOfParallelInstances() {
        return context.getNumberOfParallelSubtasks();
    }

    @Override
    public int[] getPartitionsForTopic(String topic) {
        // the fetched list is immutable, so we're creating a mutable copy in order to sort it
        final List<PartitionInfo> partitionsList = new ArrayList<>(producer.partitionsFor(topic));

        return partitionsList.stream()
                .sorted(Comparator.comparing(PartitionInfo::partition))
                .map(PartitionInfo::partition)
                .mapToInt(Integer::intValue)
                .toArray();
    }
}
