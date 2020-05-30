/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.	See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.	The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.	You may obtain a copy of the License at
 *
 *		 http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.flink.connectors.common.sink;

import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Meter;
import org.apache.flink.types.Row;

import com.alibaba.flink.connectors.common.MetricUtils;

import java.io.IOException;

/**
 * All OutputFormats should extends TupleRichOutputFormat to handle add and delete messages. For
 * example, like the RdsOutputFormat.
 */
public abstract class TupleRichOutputFormat extends RichOutputFormat<Tuple2<Boolean, Row>> {
	private Meter sinkInTps;
	private Counter delCounter;

	@Override
	public void open(int taskNumber, int numTasks) throws IOException {
		sinkInTps = MetricUtils.registerSinkInTps(getRuntimeContext());
		delCounter = getRuntimeContext().getMetricGroup().addGroup(MetricUtils.METRIC_GROUP_SINK)
				.counter("del");
	}

	@Override
	public void writeRecord(Tuple2<Boolean, Row> cRow) throws IOException {
		sinkInTps.markEvent();
		if (cRow.f0) {
			// Use Row.copy() method to avoid bug when setting object reuse mode.
			writeAddRecord(Row.copy(cRow.f1));
		} else {
			writeDeleteRecord(Row.copy(cRow.f1));
			delCounter.inc();
		}
	}

	/**
	 * Write Add record.
	 *
	 * @param row
	 * @throws IOException
	 */
	public abstract void writeAddRecord(Row row) throws IOException;

	/**
	 * Write Delete record.
	 *
	 * @param row
	 * @throws IOException
	 */
	public abstract void writeDeleteRecord(Row row) throws IOException;

	/**
	 * Get name of output format.
	 * @return name
	 */
	public abstract String getName();
}
