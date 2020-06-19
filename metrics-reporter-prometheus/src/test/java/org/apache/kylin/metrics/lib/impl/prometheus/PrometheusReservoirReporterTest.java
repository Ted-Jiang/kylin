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

package org.apache.kylin.metrics.lib.impl.prometheus;

import static org.junit.Assert.assertEquals;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metrics.lib.ActiveReservoir;
import org.apache.kylin.metrics.lib.Record;
import org.apache.kylin.metrics.lib.impl.InstantReservoir;
import org.apache.kylin.metrics.lib.impl.RecordEvent;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.rule.PowerMockRule;

import com.google.common.collect.Lists;

@PrepareForTest({ PrometheusReservoirReporter.PrometheusReservoirListener.class })
public class PrometheusReservoirReporterTest {

    @Rule
    public PowerMockRule rule = new PowerMockRule();

    private PrometheusReservoirReporter prometheusReporter;
    private ActiveReservoir reservoir;

    @Before
    public void setUp() throws Exception {
        System.setProperty(KylinConfig.KYLIN_CONF, "../examples/test_case_data/localmeta");

        PrometheusEventProducer producer = PowerMockito.mock(PrometheusEventProducer.class);
        PowerMockito.whenNew(PrometheusEventProducer.class).withAnyArguments().thenReturn(producer);

        reservoir = new InstantReservoir();
        reservoir.start();
        prometheusReporter = PrometheusReservoirReporter.forRegistry(reservoir).build();
    }

    @After
    public void after() throws Exception {
        System.clearProperty(KylinConfig.KYLIN_CONF);
    }

    @Test
    public void testUpdate() throws Exception {
        Record record = new RecordEvent("TEST");
        reservoir.update(record);
        assertEquals(0, prometheusReporter.getListener().getNRecord());

        prometheusReporter.start();
        reservoir.update(record);
        reservoir.update(record);
        assertEquals(2, prometheusReporter.getListener().getNRecord());

        prometheusReporter.stop();
        reservoir.update(record);
        assertEquals(2, prometheusReporter.getListener().getNRecord());

        prometheusReporter.start();
        PowerMockito.doThrow(new RuntimeException()).when(prometheusReporter.getListener().producer)
                .add(Lists.newArrayList(record));
        reservoir.update(record);
        assertEquals(2, prometheusReporter.getListener().getNRecord());
        assertEquals(1, prometheusReporter.getListener().getNRecordSkip());
    }
}
