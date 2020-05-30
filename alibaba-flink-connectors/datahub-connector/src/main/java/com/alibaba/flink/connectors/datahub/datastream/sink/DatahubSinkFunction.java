/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *	 http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.flink.connectors.datahub.datastream.sink;

import com.alibaba.flink.connectors.common.sink.OutputFormatSinkFunction;

/**
 * DatahubSinkFunction.
 * @param <T>
 */
public class DatahubSinkFunction<T> extends OutputFormatSinkFunction<T> {
	public DatahubSinkFunction(DatahubOutputFormat<T> slsOutputFormat) {
		super(slsOutputFormat);
	}

	public DatahubSinkFunction(
			String endPoint, String projectName, String topicName,
			String accessKeyId, String accessKeySecret,
			DatahubRecordResolver<T> serializationSchema) {
		this(new DatahubOutputFormat.Builder()
					.setEndPoint(endPoint).setAccessKey(accessKeySecret).setAccessId(accessKeyId)
					.setProjectName(projectName).setTopicName(topicName)
					.setRecordResolver(serializationSchema).build());
	}
}
