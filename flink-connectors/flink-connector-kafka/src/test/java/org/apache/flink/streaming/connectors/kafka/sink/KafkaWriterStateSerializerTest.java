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

import org.junit.Test;

import java.io.IOException;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

/**
 * Tests for serializing and deserialzing {@link KafkaWriterState} with {@link
 * KafkaWriterStateSerializer}.
 */
public class KafkaWriterStateSerializerTest {

    private static final Properties KAFKA_PRODUCER_CONFIG = new Properties();
    private static final KafkaWriterStateSerializer SERIALIZER =
            new KafkaWriterStateSerializer(KAFKA_PRODUCER_CONFIG);

    @Test
    public void testStateSerDe() throws IOException {
        final KafkaWriterState state = new KafkaWriterState(KAFKA_PRODUCER_CONFIG, 1, (short) 3);
        final byte[] serialized = SERIALIZER.serialize(state);
        assertEquals(state, SERIALIZER.deserialize(1, serialized));
    }
}
