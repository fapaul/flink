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

package org.apache.flink.api.connector.sink;

import org.apache.flink.api.java.functions.KeySelector;

import java.util.List;
import java.util.Optional;

/**
 * The {@code CommittableAggregator} is responsible for aggregator committables emitted by the
 * {@link SinkWriter}. The committables are forwarded after the aggregation to the committers (i.e.
 * {@link Committer} or {@link GlobalCommitter}).
 *
 * <p>The {@code CommittableAggregator} runs always with a parallelism equal to 1. Be aware that
 * doing heavy computations in the {@link CommittableAggregator#aggregate(List)} might decrease the
 * overall throughput of the sink significantly.
 *
 * @param <CommT> type of the incoming committable
 */
public interface CommittableAggregator<CommT> {

    /**
     * Computes a key for a committable to determine the distribution across downstream committers.
     *
     * @return the key derived from the committable
     */
    default <KEY> Optional<KeySelector<CommT, KEY>> getKeySelector() {
        return Optional.empty();
    }

    /**
     * Aggregates the committables emitted by the {@link SinkWriter}s and emits a new set of
     * committables to the downstream committers.
     *
     * @param committables committables sent by the {@link SinkWriter}s.
     * @return committables forwarded to the committers
     */
    List<CommT> aggregate(List<CommT> committables);
}
