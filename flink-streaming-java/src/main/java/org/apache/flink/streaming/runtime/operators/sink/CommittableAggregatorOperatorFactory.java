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
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;

/**
 * A {@link org.apache.flink.streaming.api.operators.StreamOperatorFactory} for {@link
 * CommitterOperator}.
 *
 * @param <CommT> the type of the committable
 */
@Internal
public class CommittableAggregatorOperatorFactory<CommT>
        extends AbstractStreamOperatorFactory<byte[]>
        implements OneInputStreamOperatorFactory<byte[], byte[]> {

    private final Sink<?, CommT, ?, ?> sink;

    public CommittableAggregatorOperatorFactory(Sink<?, CommT, ?, ?> sink) {
        this.sink = sink;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T extends StreamOperator<byte[]>> T createStreamOperator(
            StreamOperatorParameters<byte[]> parameters) {
        final CommittableAggregatorOperator<CommT> committableAggregatorOperator =
                new CommittableAggregatorOperator<>(
                        sink.getCommittableSerializer().orElseThrow(this::noSerializerFound),
                        sink.createCommittableAggregator().get());
        committableAggregatorOperator.setup(
                parameters.getContainingTask(),
                parameters.getStreamConfig(),
                parameters.getOutput());
        return (T) committableAggregatorOperator;
    }

    @Override
    @SuppressWarnings("rawtypes")
    public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
        return CommittableAggregatorOperator.class;
    }

    private IllegalStateException noSerializerFound() {
        return new IllegalStateException(
                sink.getClass()
                        + " does not implement getCommittableSerializer which is needed for any (global) committer.");
    }
}
