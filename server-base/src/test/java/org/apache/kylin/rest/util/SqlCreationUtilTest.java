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

package org.apache.kylin.rest.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.common.util.StringUtil;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.metrics.MetricsManager;
import org.apache.kylin.metrics.lib.ActiveReservoir;
import org.apache.kylin.metrics.lib.Sink;
import org.apache.kylin.metrics.lib.impl.hive.HiveSink;
import org.apache.kylin.rest.request.PrepareSqlRequest;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

public class SqlCreationUtilTest extends LocalFileMetadataTestCase {

    private static final String projectName = "project";
    private static final String cubeName = "cube";
    private static final String startTime = "2020-06-10";
    private static final String endTime = "2020-06-20";

    private static final Sink sink = new HiveSink();

    private static String eventTimeZone;

    @BeforeClass
    public static void setUp() {
        staticCreateTestMetadata();

        eventTimeZone = System.getProperty("kylin.metrics.event-time-zone");
        System.setProperty("kylin.metrics.event-time-zone", "GMT");

        Map<ActiveReservoir, List<Pair<String, Properties>>> sourceReporterBindProperties = new HashMap<>();
        MetricsManager.initMetricsManager(sink, sourceReporterBindProperties);
    }

    @AfterClass
    public static void after() throws Exception {
        staticCleanupTestMetadata();

        if (!StringUtil.isEmpty(eventTimeZone)) {
            System.setProperty("kylin.metrics.event-time-zone", eventTimeZone);
        }
    }

    @Test
    public void testCreatePrepareSqlRequestOfChartMetrics() {
        final String expectedQuerySql = "select REALIZATION, avg(QUERY_TIME_COST)\n"
                + "from KYLIN.HIVE_METRICS_QUERY_DEV\n"
                + "where PROJECT = ? and EXCEPTION = ? and KDAY_DATE >= ? and KDAY_DATE <= ? and REALIZATION = ?\n"
                + "group by REALIZATION\n";

        String categoryQ = "QUERY";
        String dimensionQ = "CUBE";
        String measureQ = "AVG_QUERY_LATENCY";
        try {
            PrepareSqlRequest sqlRequest = SqlCreationUtil.createPrepareSqlRequestOfChartMetrics(categoryQ, projectName,
                    cubeName, startTime, endTime, dimensionQ, measureQ);
            Assert.assertEquals(expectedQuerySql, sqlRequest.getSql());
        } catch (Exception e) {
            Assert.fail("category is not correct");
        }

        final String expectedJobSql = "select CUBE_NAME, avg(PER_BYTES_TIME_COST)\n"
                + "from KYLIN.HIVE_METRICS_JOB_DEV\n"
                + "where PROJECT = ? and CUBE_NAME IN (?) and KDAY_DATE >= ? and KDAY_DATE <= ?\n"
                + "group by CUBE_NAME\n";

        String categoryJ = "JOB";
        String dimensionJ = "CUBE";
        String measureJ = "AVG_JOB_BUILD_TIME";
        try {
            PrepareSqlRequest sqlRequest = SqlCreationUtil.createPrepareSqlRequestOfChartMetrics(categoryJ, projectName,
                    cubeName, startTime, endTime, dimensionJ, measureJ);
            Assert.assertEquals(expectedJobSql, sqlRequest.getSql());
        } catch (Exception e) {
            Assert.fail("category is not correct");
        }

        String categoryBad = "JOB1";
        try {
            SqlCreationUtil.createPrepareSqlRequestOfChartMetrics(categoryBad, projectName, cubeName, startTime,
                    endTime, dimensionJ, measureJ);
            Assert.fail("category is not correct");
        } catch (Exception e) {
        }
    }

    @Test
    public void testCreatePrepareSqlRequestOfTotalQueryMetrics() {
        final String expectedSql = "select count(*), avg(QUERY_TIME_COST), max(QUERY_TIME_COST), min(QUERY_TIME_COST)\n"
                + "from KYLIN.HIVE_METRICS_QUERY_DEV\n"
                + "where PROJECT = ? and EXCEPTION = ? and KDAY_DATE >= ? and KDAY_DATE <= ? and REALIZATION = ?\n";

        PrepareSqlRequest sqlRequest = SqlCreationUtil.createPrepareSqlRequestOfTotalQueryMetrics(projectName, cubeName,
                startTime, endTime);
        Assert.assertEquals(expectedSql, sqlRequest.getSql());
    }

    @Test
    public void testCreatePrepareSqlRequestOfTotalJobMetrics() {
        final String expectedSql = "select count(*), avg(PER_BYTES_TIME_COST), max(PER_BYTES_TIME_COST), min(PER_BYTES_TIME_COST), (case \n"
                + " when sum(TABLE_SIZE) = 0 then -1 \n" + " else 1.0 * sum(CUBE_SIZE) / sum(TABLE_SIZE) \n" + " end)\n"
                + "from KYLIN.HIVE_METRICS_JOB_DEV\n"
                + "where PROJECT = ? and CUBE_NAME IN (?) and KDAY_DATE >= ? and KDAY_DATE <= ?\n";

        PrepareSqlRequest sqlRequest = SqlCreationUtil.createPrepareSqlRequestOfTotalJobMetrics(projectName, cubeName,
                startTime, endTime);
        Assert.assertEquals(expectedSql, sqlRequest.getSql());
    }

    @Test
    public void testCreatePrepareSqlRequestOfCuboidHitFrequency() {
        {
            final String expectedSql = "select CUBOID_TARGET, sum(WEIGHT_PER_HIT)\n"
                    + "from KYLIN.HIVE_METRICS_QUERY_CUBE_DEV\n" + "where CUBE_NAME = ?\n" + "group by CUBOID_TARGET\n";

            PrepareSqlRequest sqlRequest = SqlCreationUtil.createPrepareSqlRequestOfCuboidHitFrequency(cubeName, false);
            Assert.assertEquals(expectedSql, sqlRequest.getSql());
        }

        {
            final String expectedSql = "select CUBOID_SOURCE, sum(WEIGHT_PER_HIT)\n"
                    + "from KYLIN.HIVE_METRICS_QUERY_CUBE_DEV\n" + "where CUBE_NAME = ?\n" + "group by CUBOID_SOURCE\n";

            PrepareSqlRequest sqlRequest = SqlCreationUtil.createPrepareSqlRequestOfCuboidHitFrequency(cubeName, true);
            Assert.assertEquals(expectedSql, sqlRequest.getSql());
        }
    }

    @Test
    public void testCreatePrepareSqlRequestOfCuboidRollingUpStats() {
        final String expectedSql = "select CUBOID_SOURCE, CUBOID_TARGET, avg(STORAGE_COUNT_AGGREGATE), avg(STORAGE_COUNT_RETURN)\n"
                + "from KYLIN.HIVE_METRICS_QUERY_CUBE_DEV\n" + "where CUBE_NAME = ?\n"
                + "group by CUBOID_SOURCE, CUBOID_TARGET\n";

        PrepareSqlRequest sqlRequest = SqlCreationUtil.createPrepareSqlRequestOfCuboidRollingUpStats(cubeName);
        Assert.assertEquals(expectedSql, sqlRequest.getSql());
    }

    @Test
    public void testCreatePrepareSqlRequestOfCuboidQueryMatchCount() {
        final String expectedSql = "select CUBOID_SOURCE, sum(WEIGHT_PER_HIT)\n"
                + "from KYLIN.HIVE_METRICS_QUERY_CUBE_DEV\n" + "where IF_MATCH = ? and CUBE_NAME = ?\n"
                + "group by CUBOID_SOURCE\n";

        PrepareSqlRequest sqlRequest = SqlCreationUtil.createPrepareSqlRequestOfCuboidQueryMatchCount(cubeName);
        Assert.assertEquals(expectedSql, sqlRequest.getSql());
    }

    @Test
    public void testCreatePrepareSqlRequestOfQueryLatencyTrend() {
        final String expectedSql = "select (case \n"
                + "  when KDAY_DATE < '1970-01-01' or (KDAY_DATE = '1970-01-01' and KTIME_HOUR < 0) then '1970-01-01 00:00:00'\n"
                + "  when KDAY_DATE < '1970-01-01' or (KDAY_DATE = '1970-01-01' and KTIME_HOUR < 5) then '1970-01-01 05:00:00'\n"
                + "  when KDAY_DATE < '1970-01-02' or (KDAY_DATE = '1970-01-02' and KTIME_HOUR < 3) then '1970-01-02 03:00:00'\n"
                + "  else '1970-01-03 07:00:00'\n" + "end) \n"
                + " as time_slot, avg(QUERY_TIME_COST) as query_latency\n" + "from KYLIN.HIVE_METRICS_QUERY_DEV\n"
                + "where REALIZATION = ?\n" + "group by (case \n"
                + "  when KDAY_DATE < '1970-01-01' or (KDAY_DATE = '1970-01-01' and KTIME_HOUR < 0) then '1970-01-01 00:00:00'\n"
                + "  when KDAY_DATE < '1970-01-01' or (KDAY_DATE = '1970-01-01' and KTIME_HOUR < 5) then '1970-01-01 05:00:00'\n"
                + "  when KDAY_DATE < '1970-01-02' or (KDAY_DATE = '1970-01-02' and KTIME_HOUR < 3) then '1970-01-02 03:00:00'\n"
                + "  else '1970-01-03 07:00:00'\n" + "end) \n" + "\n" + "order by (case \n"
                + "  when KDAY_DATE < '1970-01-01' or (KDAY_DATE = '1970-01-01' and KTIME_HOUR < 0) then '1970-01-01 00:00:00'\n"
                + "  when KDAY_DATE < '1970-01-01' or (KDAY_DATE = '1970-01-01' and KTIME_HOUR < 5) then '1970-01-01 05:00:00'\n"
                + "  when KDAY_DATE < '1970-01-02' or (KDAY_DATE = '1970-01-02' and KTIME_HOUR < 3) then '1970-01-02 03:00:00'\n"
                + "  else '1970-01-03 07:00:00'\n" + "end) \n\n";

        List<Long> optTimeList = new ArrayList<>();
        optTimeList.add(1000000L);
        optTimeList.add(20000000L);
        optTimeList.add(100000000L);
        CubeInstance cubeInstance = Mockito.mock(CubeInstance.class);
        Mockito.when(cubeInstance.getName()).thenReturn(cubeName);
        Mockito.when(cubeInstance.getCuboidOptimizedTimestamps()).thenReturn(optTimeList);

        PrepareSqlRequest sqlRequest = SqlCreationUtil.createPrepareSqlRequestOfQueryLatencyTrend(cubeInstance,
                200000000L);
        Assert.assertEquals(expectedSql, sqlRequest.getSql());
    }

    @Test
    public void testCreatePrepareSqlRequestOfStorageUsageTrend() {
        final String expectedSql = "select (case \n"
                + "  when KDAY_DATE < '1970-01-01' or (KDAY_DATE = '1970-01-01' and KTIME_HOUR < 0) then '1970-01-01 00:00:00'\n"
                + "  when KDAY_DATE < '1970-01-01' or (KDAY_DATE = '1970-01-01' and KTIME_HOUR < 5) then '1970-01-01 05:00:00'\n"
                + "  when KDAY_DATE < '1970-01-02' or (KDAY_DATE = '1970-01-02' and KTIME_HOUR < 3) then '1970-01-02 03:00:00'\n"
                + "  else '1970-01-03 07:00:00'\n" + "end) \n" + " as time_slot, (case \n"
                + " when sum(TABLE_SIZE) = 0 then -1 \n" + " else 1.0 * sum(CUBE_SIZE) / sum(TABLE_SIZE) \n"
                + " end) as expansion_rate\n" + "from KYLIN.HIVE_METRICS_JOB_DEV\n" + "where CUBE_NAME = ?\n"
                + "group by (case \n"
                + "  when KDAY_DATE < '1970-01-01' or (KDAY_DATE = '1970-01-01' and KTIME_HOUR < 0) then '1970-01-01 00:00:00'\n"
                + "  when KDAY_DATE < '1970-01-01' or (KDAY_DATE = '1970-01-01' and KTIME_HOUR < 5) then '1970-01-01 05:00:00'\n"
                + "  when KDAY_DATE < '1970-01-02' or (KDAY_DATE = '1970-01-02' and KTIME_HOUR < 3) then '1970-01-02 03:00:00'\n"
                + "  else '1970-01-03 07:00:00'\n" + "end) \n" + "\n" + "order by (case \n"
                + "  when KDAY_DATE < '1970-01-01' or (KDAY_DATE = '1970-01-01' and KTIME_HOUR < 0) then '1970-01-01 00:00:00'\n"
                + "  when KDAY_DATE < '1970-01-01' or (KDAY_DATE = '1970-01-01' and KTIME_HOUR < 5) then '1970-01-01 05:00:00'\n"
                + "  when KDAY_DATE < '1970-01-02' or (KDAY_DATE = '1970-01-02' and KTIME_HOUR < 3) then '1970-01-02 03:00:00'\n"
                + "  else '1970-01-03 07:00:00'\n" + "end) \n\n";

        List<Long> optTimeList = new ArrayList<>();
        optTimeList.add(1000000L);
        optTimeList.add(20000000L);
        optTimeList.add(100000000L);
        CubeInstance cubeInstance = Mockito.mock(CubeInstance.class);
        Mockito.when(cubeInstance.getName()).thenReturn(cubeName);
        Mockito.when(cubeInstance.getCuboidOptimizedTimestamps()).thenReturn(optTimeList);

        PrepareSqlRequest sqlRequest = SqlCreationUtil.createPrepareSqlRequestOfStorageUsageTrend(cubeInstance,
                200000000L);
        Assert.assertEquals(expectedSql, sqlRequest.getSql());
    }
}
