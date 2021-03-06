/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.flink.connectors.sls.datastream.sink;

import com.aliyun.openservices.aliyun.log.producer.LogProducer;
import com.aliyun.openservices.aliyun.log.producer.errors.ProducerException;
import org.junit.Test;
import org.mockito.Mockito;
import org.powermock.reflect.Whitebox;

/**
 * Tests for log producer provider.
 */
public class LogProducerProviderTest {

	@Test
	public void testClose() throws ProducerException, InterruptedException {
		LogProducerProvider producerProvider = new LogProducerProvider(
				"test-project", "test-endpoint", "test-ak", "test-secret", 1, 100);

		LogProducer producer = Mockito.mock(LogProducer.class);
		Whitebox.setInternalState(producerProvider, "client", producer);

		Mockito.doThrow(new InterruptedException()).doNothing().when(producer).close();
		producerProvider.closeClient();

		Mockito.verify(producer, Mockito.times(2)).close();
	}
}
