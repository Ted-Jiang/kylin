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

import java.util.Properties;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exceptions.ResourceLimitExceededException;
import org.apache.kylin.metrics.lib.Record;
import org.apache.kylin.metrics.lib.impl.RecordEvent;
import org.apache.kylin.metrics.lib.impl.TimedRecordEvent;
import org.apache.kylin.metrics.property.JobPropertyEnum;
import org.apache.kylin.metrics.property.QueryCubePropertyEnum;
import org.apache.kylin.metrics.property.QueryPropertyEnum;
import org.apache.kylin.metrics.property.QueryRPCPropertyEnum;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class PrometheusEventProducerTest {

    private static final String project = "default";
    private static final String cubeName = "ssb";

    private PrometheusEventProducer producer;

    @Before
    public void setUp() throws Exception {
        System.setProperty(KylinConfig.KYLIN_CONF, "../examples/test_case_data/localmeta");
        producer = new PrometheusEventProducer(new Properties());
    }

    @After
    public void after() throws Exception {
        producer.close();
        System.clearProperty(KylinConfig.KYLIN_CONF);
    }

    @Test
    public void testAddRecord() throws Exception {
        producer.add(mockQueryRpcCallEvent());
        assertEquals(0L, producer.getQueryRPCLatencyGauge(project, cubeName));
        producer.add(mockQueryRpcCallExceptionEvent());
        assertEquals(500L, producer.getQueryRPCLatencyGauge(project, cubeName));

        producer.add(mockQueryCubeEvent());
        producer.add(mockQueryEvent());
        assertEquals(55L, producer.getQueryLatencyGauge(project, cubeName));
        assertEquals(0L, producer.getQueryAvailabilityGauge(project, cubeName));
        assertEquals(0L, producer.getSlowQueryPercentage(project, cubeName));
        assertEquals(0L, producer.getQueryHitCachePercentage(project, cubeName));

        producer.add(mockJobEvent());
        assertEquals(7200000L, producer.getJobDurationGauge(project, cubeName, "BUILD"));
        assertEquals(500000L, producer.getBuildDictDurationGauge(project, cubeName));
        assertEquals(2000000L, producer.getDistinctColDurationGauge(project, cubeName));
        assertEquals(1500000L, producer.getInmemCubingDurationGauge(project, cubeName));
        assertEquals(1000000L, producer.getConvertHFileDurationGauge(project, cubeName));
        assertEquals(300000L, producer.getWaitResDurationGauge(project, cubeName, "BUILD"));
        assertEquals(0L, producer.getErrorJobPercentage(project, cubeName, "BUILD"));
        producer.add(mockJobExceptionEvent());
        producer.add(mockTestEvent());

        assertEquals(1L, producer.getQueryCounter(project, cubeName));
        assertEquals(0L, producer.getBadQueryCounter(project, cubeName));
        assertEquals(0L, producer.getSlowQueryCounter(project, cubeName));
        assertEquals(1L, producer.getNormalQueryCounter(project, cubeName));
        assertEquals(0L, producer.getQueryHitCacheCounter(project, cubeName));
        assertEquals(0L, producer.getScanOutOfLimitExCounter(project, cubeName));
        assertEquals(1L, producer.getEpRPCExceptionCounter(project, cubeName));
        assertEquals(2L, producer.getJobCounter(project, cubeName, "BUILD"));
        assertEquals(1L, producer.getErrorJobCounter(project, cubeName, "BUILD"));
    }

    private Record mockQueryRpcCallEvent() {
        RecordEvent metricsEvent = new TimedRecordEvent(
                KylinConfig.getInstanceFromEnv().getKylinMetricsSubjectQueryRpcCall());
        metricsEvent.put(QueryRPCPropertyEnum.PROJECT.toString(), project);
        metricsEvent.put(QueryRPCPropertyEnum.REALIZATION.toString(), cubeName);
        metricsEvent.put(QueryRPCPropertyEnum.RPC_SERVER.toString(), "localhost");
        metricsEvent.put(QueryRPCPropertyEnum.EXCEPTION.toString(), "NULL");

        metricsEvent.put(QueryRPCPropertyEnum.CALL_TIME.toString(), 50L);
        metricsEvent.put(QueryRPCPropertyEnum.SKIP_COUNT.toString(), 0L);
        metricsEvent.put(QueryRPCPropertyEnum.SCAN_COUNT.toString(), 10L);
        metricsEvent.put(QueryRPCPropertyEnum.RETURN_COUNT.toString(), 10L);
        metricsEvent.put(QueryRPCPropertyEnum.AGGR_FILTER_COUNT.toString(), 0L);
        metricsEvent.put(QueryRPCPropertyEnum.AGGR_COUNT.toString(), 0L);
        return metricsEvent;
    }

    private Record mockQueryRpcCallExceptionEvent() {
        RecordEvent metricsEvent = new TimedRecordEvent(
                KylinConfig.getInstanceFromEnv().getKylinMetricsSubjectQueryRpcCall());
        metricsEvent.put(QueryRPCPropertyEnum.PROJECT.toString(), project);
        metricsEvent.put(QueryRPCPropertyEnum.REALIZATION.toString(), cubeName);
        metricsEvent.put(QueryRPCPropertyEnum.RPC_SERVER.toString(), "localhost");
        metricsEvent.put(QueryRPCPropertyEnum.EXCEPTION.toString(), ResourceLimitExceededException.class);

        metricsEvent.put(QueryRPCPropertyEnum.CALL_TIME.toString(), 500L);
        metricsEvent.put(QueryRPCPropertyEnum.SKIP_COUNT.toString(), 0L);
        metricsEvent.put(QueryRPCPropertyEnum.SCAN_COUNT.toString(), 0L);
        metricsEvent.put(QueryRPCPropertyEnum.RETURN_COUNT.toString(), 0L);
        metricsEvent.put(QueryRPCPropertyEnum.AGGR_FILTER_COUNT.toString(), 0L);
        metricsEvent.put(QueryRPCPropertyEnum.AGGR_COUNT.toString(), 0L);
        return metricsEvent;
    }

    private Record mockQueryCubeEvent() {
        RecordEvent metricsEvent = new TimedRecordEvent(
                KylinConfig.getInstanceFromEnv().getKylinMetricsSubjectQueryCube());
        metricsEvent.put(QueryCubePropertyEnum.PROJECT.toString(), project);
        metricsEvent.put(QueryCubePropertyEnum.CUBE.toString(), cubeName);
        metricsEvent.put(QueryCubePropertyEnum.SEGMENT.toString(), "20130101000000_20131201000000");
        metricsEvent.put(QueryCubePropertyEnum.CUBOID_SOURCE.toString(), 255L);
        metricsEvent.put(QueryCubePropertyEnum.CUBOID_TARGET.toString(), 255L);
        metricsEvent.put(QueryCubePropertyEnum.IF_MATCH.toString(), true);
        metricsEvent.put(QueryCubePropertyEnum.FILTER_MASK.toString(), 0L);

        metricsEvent.put(QueryCubePropertyEnum.CALL_COUNT.toString(), 1L);
        metricsEvent.put(QueryCubePropertyEnum.TIME_SUM.toString(), 50L);
        metricsEvent.put(QueryCubePropertyEnum.TIME_MAX.toString(), 50L);
        metricsEvent.put(QueryCubePropertyEnum.SKIP_COUNT.toString(), 0L);
        metricsEvent.put(QueryCubePropertyEnum.SCAN_COUNT.toString(), 10L);
        metricsEvent.put(QueryCubePropertyEnum.RETURN_COUNT.toString(), 10L);
        metricsEvent.put(QueryCubePropertyEnum.AGGR_FILTER_COUNT.toString(), 0L);
        metricsEvent.put(QueryCubePropertyEnum.AGGR_COUNT.toString(), 0L);
        metricsEvent.put(QueryCubePropertyEnum.IF_SUCCESS.toString(), true);
        metricsEvent.put(QueryCubePropertyEnum.WEIGHT_PER_HIT.toString(), 1.0);
        return metricsEvent;
    }

    private Record mockQueryEvent() {
        RecordEvent metricsEvent = new TimedRecordEvent(KylinConfig.getInstanceFromEnv().getKylinMetricsSubjectQuery());
        metricsEvent.put(QueryPropertyEnum.USER.toString(), "ADMIN");
        metricsEvent.put(QueryPropertyEnum.ID_CODE.toString(), "123456");
        metricsEvent.put(QueryPropertyEnum.TYPE.toString(), "OLAP");
        metricsEvent.put(QueryPropertyEnum.PROJECT.toString(), project);
        metricsEvent.put(QueryPropertyEnum.REALIZATION.toString(), cubeName);
        metricsEvent.put(QueryPropertyEnum.REALIZATION_TYPE.toString(), "Cube");
        metricsEvent.put(QueryPropertyEnum.EXCEPTION.toString(), "NULL");

        metricsEvent.put(QueryPropertyEnum.TIME_COST.toString(), 55L);
        metricsEvent.put(QueryPropertyEnum.CALCITE_RETURN_COUNT.toString(), 10L);
        metricsEvent.put(QueryPropertyEnum.STORAGE_RETURN_COUNT.toString(), 10L);
        metricsEvent.put(QueryPropertyEnum.AGGR_FILTER_COUNT.toString(), 0L);
        return metricsEvent;
    }

    private Record mockJobEvent() {
        RecordEvent metricsEvent = new TimedRecordEvent(KylinConfig.getInstanceFromEnv().getKylinMetricsSubjectJob());
        metricsEvent.put(JobPropertyEnum.USER.toString(), "ADMIN");
        metricsEvent.put(JobPropertyEnum.PROJECT.toString(), project);
        metricsEvent.put(JobPropertyEnum.CUBE.toString(), cubeName);
        metricsEvent.put(JobPropertyEnum.ID_CODE.toString(), "dfaf3fafaf");
        metricsEvent.put(JobPropertyEnum.TYPE.toString(), "BUILD");
        metricsEvent.put(JobPropertyEnum.ALGORITHM.toString(), "INMEM");

        metricsEvent.put(JobPropertyEnum.SOURCE_SIZE.toString(), 1000000000L);
        metricsEvent.put(JobPropertyEnum.CUBE_SIZE.toString(), 10000000L);
        metricsEvent.put(JobPropertyEnum.BUILD_DURATION.toString(), 7200000L);
        metricsEvent.put(JobPropertyEnum.WAIT_RESOURCE_TIME.toString(), 300000L);
        metricsEvent.put(JobPropertyEnum.PER_BYTES_TIME_COST.toString(), 0.0072);
        metricsEvent.put(JobPropertyEnum.STEP_DURATION_DISTINCT_COLUMNS.toString(), 2000000L);
        metricsEvent.put(JobPropertyEnum.STEP_DURATION_DICTIONARY.toString(), 500000L);
        metricsEvent.put(JobPropertyEnum.STEP_DURATION_INMEM_CUBING.toString(), 1500000L);
        metricsEvent.put(JobPropertyEnum.STEP_DURATION_HFILE_CONVERT.toString(), 1000000L);
        return metricsEvent;
    }

    private Record mockJobExceptionEvent() {
        RecordEvent metricsEvent = new TimedRecordEvent(
                KylinConfig.getInstanceFromEnv().getKylinMetricsSubjectJobException());
        metricsEvent.put(JobPropertyEnum.USER.toString(), "ADMIN");
        metricsEvent.put(JobPropertyEnum.PROJECT.toString(), project);
        metricsEvent.put(JobPropertyEnum.CUBE.toString(), cubeName);
        metricsEvent.put(JobPropertyEnum.ID_CODE.toString(), "dfaf3fafaf");
        metricsEvent.put(JobPropertyEnum.TYPE.toString(), "BUILD");
        metricsEvent.put(JobPropertyEnum.ALGORITHM.toString(), "INMEM");
        metricsEvent.put(JobPropertyEnum.EXCEPTION.toString(),
                "org.apache.hadoop.security.authorize.AuthorizationException");
        return metricsEvent;
    }

    private Record mockTestEvent() {
        RecordEvent metricsEvent = new TimedRecordEvent("TEST");
        return metricsEvent;
    }
}
