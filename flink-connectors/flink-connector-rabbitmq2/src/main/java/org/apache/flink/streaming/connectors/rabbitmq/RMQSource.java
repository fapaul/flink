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
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.connector.base.source.reader.synchronization.FutureNotifier;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

import java.io.IOException;
import java.util.function.Supplier;

/**
 *
 * @param <OUT>
 */
public class RMQSource<OUT> implements Source<OUT, RMQSourceSplit, RMQSourceState> {

	private final RMQConnectionConfig rmqConnectionConfig;
	private final String queueName;
	private final DeserializationSchema<OUT> deserializationSchema;

	public RMQSource(RMQConnectionConfig rmqConnectionConfig, String queueName, DeserializationSchema<OUT> deserializationSchema) {
		this.rmqConnectionConfig = rmqConnectionConfig;
		this.queueName = queueName;
		this.deserializationSchema = deserializationSchema;
	}

	@Override
	public Boundedness getBoundedness() {
		return Boundedness.CONTINUOUS_UNBOUNDED;
	}

	@Override
	public SourceReader<OUT, RMQSourceSplit> createReader(SourceReaderContext readerContext) {
		FutureNotifier futureNotifier = new FutureNotifier();
		FutureCompletingBlockingQueue<RecordsWithSplitIds<Tuple2<OUT, Long>>> elementsQueue =
			new FutureCompletingBlockingQueue<>(futureNotifier);
		RMQRecordEmitter<OUT> recordEmitter = new RMQRecordEmitter<>();

		try {
			Channel channel = setUpChannel();
			Supplier<SplitReader<Tuple2<OUT, Long>, RMQSourceSplit>> splitReaderFactory =
				() -> new RMQSplitReader<>(queueName, channel, deserializationSchema);
			return new RMQSourceReader<OUT>(
				futureNotifier,
				elementsQueue,
				channel,
				deserializationSchema,
				splitReaderFactory,
				recordEmitter,
				readerContext);
		} catch (Exception e) {
			throw new RuntimeException("Could not set up Channel to RMQ.");
		}
	}

	@Override
	public SplitEnumerator<RMQSourceSplit, RMQSourceState> createEnumerator(SplitEnumeratorContext<RMQSourceSplit> enumContext) {
		return new RMQSourceEnumerator();
	}

	@Override
	public SplitEnumerator<RMQSourceSplit, RMQSourceState> restoreEnumerator(SplitEnumeratorContext<RMQSourceSplit> enumContext, RMQSourceState checkpoint) throws IOException {
		return new RMQSourceEnumerator();
	}

	@Override
	public SimpleVersionedSerializer<RMQSourceSplit> getSplitSerializer() {
		return new RMQDummySplitSerializer();
	}

	@Override
	public SimpleVersionedSerializer<RMQSourceState> getEnumeratorCheckpointSerializer() {
		return new RMQDummyStateSerializer();
	}

	private Channel setUpChannel() throws Exception {
		Connection connection = rmqConnectionConfig.getConnectionFactory().newConnection();
		Channel channel = connection.createChannel();
		setupQueue().setUp(channel, queueName);
		return channel;
	}

	protected QueueSetup setupQueue() {
		return this::declareQueueDefaults;
	}

	private void declareQueueDefaults(Channel channel, String queueName) throws IOException {
		channel.queueDeclare(queueName, true, false, false, null);
	}
}
