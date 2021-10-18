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

package org.apache.flink.streaming.runtime.operators.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.sink.CommittableAggregator;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/**
 * TODO: write aggregator java doc.
 *
 * @param <CommT> the type of the committable
 */
@Internal
public class CommittableAggregatorOperator<CommT> extends AbstractStreamOperator<byte[]>
        implements OneInputStreamOperator<byte[], byte[]>, BoundedOneInput {

    private final SimpleVersionedSerializer<CommT> committableSerializer;
    private final CommittableAggregator<CommT> committableAggregator;

    public CommittableAggregatorOperator(
            SimpleVersionedSerializer<CommT> committableSerializer,
            CommittableAggregator<CommT> committableAggregator) {
        this.committableSerializer = committableSerializer;
        this.committableAggregator = committableAggregator;
    }

    @Override
    public void endInput() throws Exception {}

    @Override
    public void processElement(StreamRecord<byte[]> element) throws Exception {
        // TODO: aggregate committables and emit on checkpoint
        output.collect(element);
    }
}
