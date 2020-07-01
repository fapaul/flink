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
import org.apache.flink.util.Collector;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.QueueingConsumer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @param <OUT>
 */
public class RMQSplitReader<OUT> implements SplitReader<Tuple2<OUT, Long>, RMQSourceSplit> {

	private static final boolean AUTO_ACK = false;

	private final Channel channel;
	private final QueueingConsumer consumer;
	private final DeserializationSchema<OUT> schema;

	public RMQSplitReader(String queueName, Channel channel, DeserializationSchema<OUT> schema) {
		this.schema = schema;
		this.channel = channel;
		this.consumer = new QueueingConsumer(channel);
		try {
			this.channel.txSelect();
			channel.basicConsume(queueName, AUTO_ACK, consumer);
		} catch (IOException e) {
			throw new RuntimeException("Could set up transaction mode for RMQ channel", e);
		}
	}

	@Override
	public RecordsWithSplitIds<Tuple2<OUT, Long>> fetch() throws InterruptedException, IOException {
		QueueingConsumer.Delivery delivery = consumer.nextDelivery();
		long timestamp = delivery.getProperties().getTimestamp().getTime();
		SimpleCollector<OUT> collector = new SimpleCollector<>();
		schema.deserialize(delivery.getBody(), collector);
		RMQDummySplitRecords<OUT> recordsWithSplitIds = new RMQDummySplitRecords<>();

		List<Tuple2<OUT, Long>> tuples = collector.getRecords().stream()
			.map((record) -> new Tuple2<>(record, timestamp))
			.collect(Collectors.toList());

		channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);

		recordsWithSplitIds.addRecords(tuples);

		return recordsWithSplitIds;
	}

	@Override
	public void handleSplitsChanges(Queue<SplitsChange<RMQSourceSplit>> splitsChanges) {
		splitsChanges.poll();
	}

	@Override
	public void wakeUp() {

	}

	private static class RMQDummySplitRecords<OUT> implements RecordsWithSplitIds<Tuple2<OUT, Long>> {

		private static final String SPLIT_ID = "test";
		private static final Collection<String> SPLITS_IDS = Collections.singletonList(SPLIT_ID);

		private final Set<String> finishedSplits;
		private final Map<String, Collection<Tuple2<OUT, Long>>> recordsBySplits;

		public RMQDummySplitRecords() {
			this.recordsBySplits = new HashMap<>();
			this.recordsBySplits.put(SPLIT_ID, new ArrayList<>());
			this.finishedSplits = new HashSet<>();
		}

		@Override
		public Collection<String> splitIds() {
			return SPLITS_IDS;
		}

		@Override
		public Map<String, Collection<Tuple2<OUT, Long>>> recordsBySplits() {
			return this.recordsBySplits;
		}

		@Override
		public Set<String> finishedSplits() {
			return finishedSplits;
		}

		public void addRecords(List<Tuple2<OUT, Long>> records) {
			recordsBySplits.get(SPLIT_ID).addAll(records);
		}
	}

	private static class SimpleCollector<T> implements Collector<T> {
		private final List<T> records = new ArrayList<>();

		@Override
		public void collect(T record) {
			records.add(record);
		}

		@Override
		public void close() {

		}

		private List<T> getRecords() {
			return records;
		}
	}
}
