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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.QueueingConsumer;

import java.io.IOException;
import java.util.Queue;

public class RMQSplitReader<OUT> implements SplitReader<Tuple2<OUT, Long>, RMQSourceSplit> {

	private final Channel channel;
	private final Consumer consumer;
	private final String queueName;
	private final DeserializationSchema<OUT> schema;

	public RMQSplitReader(String queueName, Channel channel, DeserializationSchema<OUT> schema) {
		this.queueName = queueName;
		this.schema = schema;
		this.channel = channel;
		this.consumer = new QueueingConsumer(channel);
	}

	@Override
	public RecordsWithSplitIds<Tuple2<OUT, Long>> fetch() throws InterruptedException, IOException {
		this.channel.txSelect();
		channel.basicConsume(queueName, true, consumer);
		return null;
	}

	@Override
	public void handleSplitsChanges(Queue<SplitsChange<RMQSourceSplit>> splitsChanges) {

	}

	@Override
	public void wakeUp() {

	}
}
