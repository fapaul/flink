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

import org.apache.flink.api.connector.sink.Committer;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.errors.InvalidTxnStateException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * Committer implementation for {@link KafkaSink}
 *
 * <p>The committer is responsible to finalize the Kafka transactions by committing them.
 */
public class KafkaCommitter implements Committer<KafkaCommittable> {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaCommitter.class);

    @Override
    public List<KafkaCommittable> commit(List<KafkaCommittable> committables) throws IOException {
        committables.forEach(KafkaCommitter::commitTransaction);
        return Collections.emptyList();
    }

    @Override
    public void close() throws Exception {}

    private static void commitTransaction(KafkaCommittable committable) {
        LOG.debug(
                "Committing Kafka transaction {}",
                committable.getKafkaProducerConfig().get(ProducerConfig.TRANSACTIONAL_ID_CONFIG));
        try (FlinkKafkaInternalProducer<?, ?> producer =
                new FlinkKafkaInternalProducer<>(committable.getKafkaProducerConfig())) {
            producer.resumeTransaction(committable.getProducerId(), committable.getEpoch());
            producer.commitTransaction();
        } catch (InvalidTxnStateException | ProducerFencedException e) {
            // That means we have committed this transaction before.
            LOG.warn(
                    "Encountered error {} while recovering transaction {}. "
                            + "Presumably this transaction has been already committed before",
                    e,
                    committable);
        }
    }
}
