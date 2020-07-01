/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.streaming.connectors.rabbitmq;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.connector.base.source.reader.synchronization.FutureNotifier;

import com.rabbitmq.client.Channel;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;

public class RMQSourceReader<OUT> extends SingleThreadMultiplexSourceReaderBase<Tuple2<OUT, Long>, OUT, RMQSourceSplit, RMQSourceState> {

	private final Channel channel;

	public RMQSourceReader(
		FutureNotifier futureNotifier,
		FutureCompletingBlockingQueue<RecordsWithSplitIds<Tuple2<OUT, Long>>> elementsQueue,
		Channel channel,
		DeserializationSchema<OUT> deserializationSchema,
		Supplier<SplitReader<Tuple2<OUT, Long>, RMQSourceSplit>> splitReaderFactory,
		RecordEmitter<Tuple2<OUT, Long>, OUT, RMQSourceState> recordEmitter,
		SourceReaderContext context) throws Exception {
		super(futureNotifier, elementsQueue, splitReaderFactory, recordEmitter, new Configuration(), context);
		this.channel = channel;
	}

	@Override
	protected void onSplitFinished(Collection<String> finishedSplitIds) {

	}

	@Override
	protected RMQSourceState initializedState(RMQSourceSplit split) {
		return new RMQSourceState();
	}

	@Override
	protected RMQSourceSplit toSplitType(String splitId, RMQSourceState splitState) {
		return new RMQSourceSplit();
	}

	@Override
	public List<RMQSourceSplit> snapshotState() {
		try {
			channel.txCommit();
		} catch (IOException e) {
			throw new RuntimeException("Messages could not be acknowledged during checkpoint creation.", e);
		}
		return new ArrayList<RMQSourceSplit>();
	}
}
