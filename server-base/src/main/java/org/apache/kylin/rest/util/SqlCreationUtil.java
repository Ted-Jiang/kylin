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

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.StringUtil;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.metrics.MetricsManager;
import org.apache.kylin.metrics.lib.impl.RecordEventTimeDetail;
import org.apache.kylin.metrics.lib.impl.TimePropertyEnum;
import org.apache.kylin.metrics.property.JobPropertyEnum;
import org.apache.kylin.metrics.property.QueryCubePropertyEnum;
import org.apache.kylin.metrics.property.QueryPropertyEnum;
import org.apache.kylin.rest.request.PrepareSqlRequest;
import org.apache.kylin.rest.request.PrepareSqlRequest.StateParam;
import org.apache.kylin.shaded.com.google.common.collect.Lists;
import org.apache.kylin.shaded.com.google.common.collect.Maps;

public class SqlCreationUtil {

    public static PrepareSqlRequest createPrepareSqlRequestOfChartMetrics(String category, String projectName,
            String cubeName, String startTime, String endTime, String dimension, String measure) throws Exception {
        CategoryEnum categoryEnum = CategoryEnum.valueOf(category);
        switch (categoryEnum) {
        case QUERY:
            String[] dimensionsQuery = new String[] { QueryDimensionEnum.valueOf(dimension).toSQL() };
            String[] measuresQuery = new String[] { QueryMeasureEnum.valueOf(measure).toSQL() };
            return createPrepareSqlRequestOfQueryMetrics(projectName, cubeName, startTime, endTime, dimensionsQuery,
                    measuresQuery);
        case JOB:
            String[] dimensionsJob = new String[] { JobDimensionEnum.valueOf(dimension).toSQL() };
            String[] measuresJob = new String[] { JobMeasureEnum.valueOf(measure).toSQL() };
            return createPrepareSqlRequestOfJobMetrics(projectName, cubeName, startTime, endTime, dimensionsJob,
                    measuresJob);
        default:
            throw new Exception("Category should either be QUERY or JOB");
        }
    }

    public static PrepareSqlRequest createPrepareSqlRequestOfTotalQueryMetrics(String projectName, String cubeName,
            String startTime, String endTime) {
        String[] measures = new String[] { QueryMeasureEnum.QUERY_COUNT.toSQL(),
                QueryMeasureEnum.AVG_QUERY_LATENCY.toSQL(), QueryMeasureEnum.MAX_QUERY_LATENCY.toSQL(),
                QueryMeasureEnum.MIN_QUERY_LATENCY.toSQL() };

        return createPrepareSqlRequestOfQueryMetrics(projectName, cubeName, startTime, endTime, null, measures);
    }

    public static PrepareSqlRequest createPrepareSqlRequestOfQueryMetrics(String projectName, String cubeName,
            String startTime, String endTime, String[] dimensions, String[] measures) {
        String table = MetricsManager
                .getSystemTableFromSubject(KylinConfig.getInstanceFromEnv().getKylinMetricsSubjectQuery());

        Map<String, StateParam> filterMap = Maps.newHashMap();
        if (StringUtil.isEmpty(projectName)) {
            addFilter(filterMap, QueryPropertyEnum.PROJECT.toString(), "<>", MetricsManager.SYSTEM_PROJECT,
                    String.class.getName());
        } else {
            addFilter(filterMap, QueryPropertyEnum.PROJECT.toString(), "=", projectName.toUpperCase(Locale.ROOT),
                    String.class.getName());
        }
        if (!StringUtil.isEmpty(cubeName)) {
            addFilter(filterMap, QueryPropertyEnum.REALIZATION.toString(), "=", cubeName, String.class.getName());
        }
        addFilter(filterMap, QueryPropertyEnum.EXCEPTION.toString(), "=", "NULL", String.class.getName());

        addFilter(filterMap, TimePropertyEnum.DAY_DATE.toString(), ">=", startTime, String.class.getName());
        addFilter(filterMap, TimePropertyEnum.DAY_DATE.toString(), "<=", endTime, String.class.getName());

        return SqlCreationUtil.createPrepareSqlRequest(table, dimensions, measures, filterMap);
    }

    public static PrepareSqlRequest createPrepareSqlRequestOfTotalJobMetrics(String projectName, String cubeName,
            String startTime, String endTime) {
        String[] measures = new String[] { JobMeasureEnum.JOB_COUNT.toSQL(), JobMeasureEnum.AVG_JOB_BUILD_TIME.toSQL(),
                JobMeasureEnum.MAX_JOB_BUILD_TIME.toSQL(), JobMeasureEnum.MIN_JOB_BUILD_TIME.toSQL(),
                JobMeasureEnum.EXPANSION_RATE.toSQL() };

        return createPrepareSqlRequestOfJobMetrics(projectName, cubeName, startTime, endTime, null, measures);
    }

    public static PrepareSqlRequest createPrepareSqlRequestOfJobMetrics(String projectName, String cubeName,
            String startTime, String endTime, String[] dimensions, String[] measures) {
        String table = MetricsManager
                .getSystemTableFromSubject(KylinConfig.getInstanceFromEnv().getKylinMetricsSubjectJob());

        Map<String, StateParam> filterMap = Maps.newHashMap();
        if (StringUtil.isEmpty(projectName)) {
            addFilter(filterMap, JobPropertyEnum.PROJECT.toString(), "<>", MetricsManager.SYSTEM_PROJECT,
                    String.class.getName());
        } else {
            addFilter(filterMap, JobPropertyEnum.PROJECT.toString(), "=", projectName.toUpperCase(Locale.ROOT),
                    String.class.getName());
        }
        if (!StringUtil.isEmpty(cubeName)) {
            addFilter(filterMap, JobPropertyEnum.CUBE.toString(), "IN", cubeName, String.class.getName());
        }

        addFilter(filterMap, TimePropertyEnum.DAY_DATE.toString(), ">=", startTime, String.class.getName());
        addFilter(filterMap, TimePropertyEnum.DAY_DATE.toString(), "<=", endTime, String.class.getName());

        return SqlCreationUtil.createPrepareSqlRequest(table, dimensions, measures, filterMap);
    }

    public static PrepareSqlRequest createPrepareSqlRequestOfCuboidHitFrequency(String cubeName,
            boolean isCuboidSource) {
        String table = MetricsManager
                .getSystemTableFromSubject(KylinConfig.getInstanceFromEnv().getKylinMetricsSubjectQueryCube());

        String[] dimensions = new String[1];
        dimensions[0] = isCuboidSource ? QueryCubePropertyEnum.CUBOID_SOURCE.toString()
                : QueryCubePropertyEnum.CUBOID_TARGET.toString();

        String[] measures = new String[1];
        measures[0] = "sum(" + QueryCubePropertyEnum.WEIGHT_PER_HIT.toString() + ")";

        Map<String, StateParam> filterMap = Maps.newHashMap();
        addFilter(filterMap, QueryCubePropertyEnum.CUBE.toString(), "=", cubeName, String.class.getName());

        return createPrepareSqlRequest(table, dimensions, measures, filterMap);
    }

    public static PrepareSqlRequest createPrepareSqlRequestOfCuboidRollingUpStats(String cubeName) {
        String table = MetricsManager
                .getSystemTableFromSubject(KylinConfig.getInstanceFromEnv().getKylinMetricsSubjectQueryCube());

        String[] dimensions = new String[2];
        dimensions[0] = QueryCubePropertyEnum.CUBOID_SOURCE.toString();
        dimensions[1] = QueryCubePropertyEnum.CUBOID_TARGET.toString();

        String[] measures = new String[2];
        measures[0] = "avg(" + QueryCubePropertyEnum.AGGR_COUNT.toString() + ")";
        measures[1] = "avg(" + QueryCubePropertyEnum.RETURN_COUNT.toString() + ")";

        Map<String, StateParam> filterMap = Maps.newHashMap();
        addFilter(filterMap, QueryCubePropertyEnum.CUBE.toString(), "=", cubeName, String.class.getName());

        return createPrepareSqlRequest(table, dimensions, measures, filterMap);
    }

    public static PrepareSqlRequest createPrepareSqlRequestOfCuboidQueryMatchCount(String cubeName) {
        String table = MetricsManager
                .getSystemTableFromSubject(KylinConfig.getInstanceFromEnv().getKylinMetricsSubjectQueryCube());

        String[] dimensions = new String[1];
        dimensions[0] = QueryCubePropertyEnum.CUBOID_SOURCE.toString();

        String[] measures = new String[1];
        measures[0] = "sum(" + QueryCubePropertyEnum.WEIGHT_PER_HIT.toString() + ")";

        Map<String, StateParam> filterMap = Maps.newHashMap();
        addFilter(filterMap, QueryCubePropertyEnum.CUBE.toString(), "=", cubeName, String.class.getName());
        addFilter(filterMap, QueryCubePropertyEnum.IF_MATCH.toString(), "=", "true", Boolean.class.getName());

        return createPrepareSqlRequest(table, dimensions, measures, filterMap);
    }

    public static PrepareSqlRequest createPrepareSqlRequestOfQueryLatencyTrend(CubeInstance cube, long endTimestamp) {
        String table = MetricsManager
                .getSystemTableFromSubject(KylinConfig.getInstanceFromEnv().getKylinMetricsSubjectQuery());

        String[] groupBys = new String[1];
        String[] dimensions = new String[1];
        groupBys[0] = getTimeSlotStringForOptimizationTrend(cube, endTimestamp);
        dimensions[0] = groupBys[0] + " as time_slot";

        String[] measures = new String[1];
        measures[0] = "avg(" + QueryPropertyEnum.TIME_COST.toString() + ") as query_latency";

        Map<String, StateParam> filterMap = Maps.newHashMap();
        addFilter(filterMap, QueryPropertyEnum.REALIZATION.toString(), "=", cube.getName(), String.class.getName());

        return createPrepareSqlRequest(table, dimensions, measures, filterMap, groupBys, groupBys);
    }

    public static PrepareSqlRequest createPrepareSqlRequestOfStorageUsageTrend(CubeInstance cube, long endTimestamp) {
        String table = MetricsManager
                .getSystemTableFromSubject(KylinConfig.getInstanceFromEnv().getKylinMetricsSubjectJob());

        String[] groupBys = new String[1];
        String[] dimensions = new String[1];
        groupBys[0] = getTimeSlotStringForOptimizationTrend(cube, endTimestamp);
        dimensions[0] = groupBys[0] + " as time_slot";

        String[] measures = new String[1];
        measures[0] = getExpansionRateMetric() + " as expansion_rate";

        Map<String, StateParam> filterMap = Maps.newHashMap();
        addFilter(filterMap, JobPropertyEnum.CUBE.toString(), "=", cube.getName(), String.class.getName());

        return createPrepareSqlRequest(table, dimensions, measures, filterMap, groupBys, groupBys);
    }

    private static String getExpansionRateMetric() {
        String sourceSizeStr = JobPropertyEnum.SOURCE_SIZE.toString();
        String cubeSizeStr = JobPropertyEnum.CUBE_SIZE.toString();
        return String.format(Locale.ROOT, "(case \n when sum(%s) = 0 then -1 \n else 1.0 * sum(%s) / sum(%s) \n end)",
                sourceSizeStr, cubeSizeStr, sourceSizeStr);
    }

    private static String getTimeSlotStringForOptimizationTrend(CubeInstance cube, long endTimestamp) {
        List<Long> optTimeList = cube.getCuboidOptimizedTimestamps();
        List<RecordEventTimeDetail> optTimeSerial = optTimeList.stream().map(RecordEventTimeDetail::new)
                .collect(Collectors.toList());
        RecordEventTimeDetail endTime = new RecordEventTimeDetail(endTimestamp);
        String endTimeStr = String.format(Locale.ROOT, "'%s %02d:00:00'", endTime.date, endTime.hour);
        if (optTimeSerial.isEmpty()) {
            return endTimeStr;
        }
        String dayDateStr = TimePropertyEnum.DAY_DATE.toString();
        String timeHourStr = TimePropertyEnum.TIME_HOUR.toString();
        StringBuilder sb = new StringBuilder();
        sb.append("(case \n");
        for (RecordEventTimeDetail optTime : optTimeSerial) {
            String whenStr = String.format(Locale.ROOT,
                    "  when %s < '%s' or (%s = '%s' and %s < %d) then '%s %02d:00:00'\n", dayDateStr, optTime.date,
                    dayDateStr, optTime.date, timeHourStr, optTime.hour, optTime.date, optTime.hour);
            sb.append(whenStr);
        }
        String elseStr = String.format(Locale.ROOT, "  else %s\n", endTimeStr);
        sb.append(elseStr);
        sb.append("end) \n");
        return sb.toString();
    }

    private static PrepareSqlRequest createPrepareSqlRequest(String table, String[] dimensions, String[] measures,
            Map<String, StateParam> filterMap) {
        return createPrepareSqlRequest(table, dimensions, measures, filterMap, dimensions, null);
    }

    private static PrepareSqlRequest createPrepareSqlRequest(String table, String[] dimensions, String[] measures,
            Map<String, StateParam> filterMap, String[] groupBys, String[] orderBys) {
        PrepareSqlRequest sqlRequest = new PrepareSqlRequest();
        sqlRequest.setProject(MetricsManager.SYSTEM_PROJECT);

        StringBuilder sqlBuilder = new StringBuilder();

        String dimPart = concatElements(dimensions);
        String measurePart = concatElements(measures);

        String filterPart = "";
        if (filterMap != null && !filterMap.isEmpty()) {
            List<Map.Entry<String, StateParam>> filterList = Lists.newArrayList(filterMap.entrySet());

            filterPart = filterList.stream().map(Map.Entry::getKey).collect(Collectors.joining(" and "));

            StateParam[] params = filterList.stream().map(Map.Entry::getValue).collect(Collectors.toList())
                    .toArray(new StateParam[filterMap.size()]);
            sqlRequest.setParams(params);
        }

        String groupByPart = concatElements(groupBys);
        String orderByPart = concatElements(orderBys);

        sqlBuilder.append("select ");
        if (!dimPart.isEmpty()) {
            sqlBuilder.append(dimPart);
        }
        if (!measurePart.isEmpty()) {
            if (!dimPart.isEmpty()) {
                sqlBuilder.append(", ");
            }
            sqlBuilder.append(measurePart);
        }
        sqlBuilder.append("\n").append("from ").append(table).append("\n");
        if (!filterPart.isEmpty()) {
            sqlBuilder.append("where ").append(filterPart).append("\n");
        }
        if (!groupByPart.isEmpty()) {
            sqlBuilder.append("group by ").append(groupByPart).append("\n");
        }
        if (!orderByPart.isEmpty()) {
            sqlBuilder.append("order by ").append(orderByPart).append("\n");
        }

        sqlRequest.setSql(sqlBuilder.toString());

        return sqlRequest;
    }

    private static String concatElements(String[] elements) {
        return elements != null && elements.length > 0 ? StringUtil.join(Arrays.asList(elements), ", ") : "";
    }

    private static void addFilter(Map<String, StateParam> filterMap, String keyName, String compareSign, String value,
            String className) {
        StateParam stateParam = new StateParam();
        stateParam.setClassName(className);
        stateParam.setValue(value);
        String mark = compareSign.equalsIgnoreCase("IN") ? "(?)" : "?";
        filterMap.put(String.format(Locale.ROOT, "%s %s %s", keyName, compareSign, mark), stateParam);
    }

    private enum CategoryEnum {
        QUERY, JOB
    }

    private enum QueryDimensionEnum {
        PROJECT(QueryPropertyEnum.PROJECT.toString()), //
        CUBE(QueryPropertyEnum.REALIZATION.toString()), //
        DAY(TimePropertyEnum.DAY_DATE.toString()), //
        WEEK(TimePropertyEnum.WEEK_BEGIN_DATE.toString()), //
        MONTH(TimePropertyEnum.MONTH.toString());

        private final String sql;

        QueryDimensionEnum(String sql) {
            this.sql = sql;
        }

        public String toSQL() {
            return this.sql;
        }
    }

    private enum JobDimensionEnum {
        PROJECT(JobPropertyEnum.PROJECT.toString()), //
        CUBE(JobPropertyEnum.CUBE.toString()), //
        DAY(TimePropertyEnum.DAY_DATE.toString()), //
        WEEK(TimePropertyEnum.WEEK_BEGIN_DATE.toString()), //
        MONTH(TimePropertyEnum.MONTH.toString());

        private final String sql;

        JobDimensionEnum(String sql) {
            this.sql = sql;
        }

        public String toSQL() {
            return this.sql;
        }
    }

    private enum QueryMeasureEnum {
        QUERY_COUNT("count(*)"), //
        AVG_QUERY_LATENCY("avg(" + QueryPropertyEnum.TIME_COST.toString() + ")"), //
        MAX_QUERY_LATENCY("max(" + QueryPropertyEnum.TIME_COST.toString() + ")"), //
        MIN_QUERY_LATENCY("min(" + QueryPropertyEnum.TIME_COST.toString() + ")");

        private final String sql;

        QueryMeasureEnum(String sql) {
            this.sql = sql;
        }

        public String toSQL() {
            return this.sql;
        }
    }

    private enum JobMeasureEnum {
        JOB_COUNT("count(*)"), //
        AVG_JOB_BUILD_TIME("avg(" + JobPropertyEnum.PER_BYTES_TIME_COST.toString() + ")"), //
        MAX_JOB_BUILD_TIME("max(" + JobPropertyEnum.PER_BYTES_TIME_COST.toString() + ")"), //
        MIN_JOB_BUILD_TIME("min(" + JobPropertyEnum.PER_BYTES_TIME_COST.toString() + ")"), //
        EXPANSION_RATE(getExpansionRateMetric());

        private final String sql;

        JobMeasureEnum(String sql) {
            this.sql = sql;
        }

        public String toSQL() {
            return this.sql;
        }
    }
}
