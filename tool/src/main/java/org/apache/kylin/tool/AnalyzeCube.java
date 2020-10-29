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
package org.apache.kylin.tool;


import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.TreeMap;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.MailTemplateProvider;
import org.apache.kylin.common.util.MailService;
import org.apache.kylin.common.util.OptionsHelper;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.job.execution.ExecutableManager;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.project.ProjectManager;
import org.apache.kylin.metadata.project.RealizationEntry;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.kylin.metadata.realization.RealizationType;
import org.apache.kylin.metrics.MetricsManager;
import org.apache.kylin.metrics.lib.impl.hive.HiveSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;


public class AnalyzeCube {
    private static final Logger logger = LoggerFactory.getLogger(AnalyzeCube.class);
    private static final String PROJECTNAME = MetricsManager.SYSTEM_PROJECT;
    private static final int DEFAULT_QUERY_DAYS = 30;
    private static final int DEFAULT_PARTITION_MONTH = 3;
    private static final int DEFAULT_DAYS = 30;
    private static final int DEFAULT_SEGMENT_NUM = 40;
    private static final int DEFAULT_TOPNUMBER = 10;
    private static final double DEFAULT_PERCENT = 0.1;

    @SuppressWarnings("static-access")
    protected static final Option OPTION_KYLIN_CONFIG_URI = OptionBuilder.withArgName("kylinConfigUri").hasArg()
            .isRequired(true).withDescription("Kylin Config uri for finding related cubes").create("kylinConfigUri");

    @SuppressWarnings("static-access")
    protected static final Option OPTION_USER = OptionBuilder.withArgName("user").hasArg().isRequired(true)
            .withDescription("user for authentication").create("user");

    @SuppressWarnings("static-access")
    protected static final Option OPTION_PASSWORD = OptionBuilder.withArgName("password").hasArg().isRequired(true)
            .withDescription("password for authentication").create("password");

    @SuppressWarnings("static-access")
    protected static final Option OPTION_RECEIVERS = OptionBuilder.withArgName("receivers").hasArg().isRequired(true)
            .withDescription("email address for receivers").create("receivers");

    @SuppressWarnings("static-access")
    private static final Option OPTION_PARTITION_MONTH = OptionBuilder.withArgName("partitionMonth").hasArg().
            isRequired(false).withDescription("Get how many new cubes created by partitionMonth").create("partitionMonth");
    @SuppressWarnings("static-access")
    private static final Option OPTION_DAYS = OptionBuilder.withArgName("days").hasArg().
            isRequired(false).withDescription("Get time n days before").create("days");
    @SuppressWarnings("static-access")
    private static final Option OPTION_SEGMENT_NUM = OptionBuilder.withArgName("segmentNum").hasArg().

            isRequired(false).withDescription("Get cubes which has more than the segment  number that we define").create("segmentNum");
    @SuppressWarnings("static-access")
    private static final Option OPTION_TOPNUMBER = OptionBuilder.withArgName("topNumber").hasArg().

            isRequired(false).withDescription("the number  show for us from query rest").create("topNumber");
    @SuppressWarnings("static-access")
    private static final Option OPTION_PERCENT = OptionBuilder.withArgName("percent").hasArg().

            isRequired(false).withDescription("Get cubes, for which the ratio of built cuboids that never hit is larger than a percent").create("percent");
    @SuppressWarnings("static-access")

    private static final Option OPTION_QUERY_DAYS = OptionBuilder.withArgName("queryDays").hasArg().

            isRequired(false).withDescription("Get cubes which are not queried within the time we defined by dyas").create("queryDays");

    public static void main(String[] args) throws Exception {
        Options options = new Options();
        OptionsHelper optionsHelper = new OptionsHelper();
        options.addOption(OPTION_KYLIN_CONFIG_URI);
        options.addOption(OPTION_USER);
        options.addOption(OPTION_PASSWORD);
        options.addOption(OPTION_RECEIVERS);
        options.addOption(OPTION_PARTITION_MONTH);
        options.addOption(OPTION_DAYS);
        options.addOption(OPTION_SEGMENT_NUM);
        options.addOption(OPTION_TOPNUMBER);
        options.addOption(OPTION_PERCENT);
        options.addOption(OPTION_QUERY_DAYS);

        try {
            optionsHelper.parseOptions(options, args);
        } catch (ParseException e) {
            e.printStackTrace();
        }

        String kylinConfigUri = optionsHelper.getOptionValue(OPTION_KYLIN_CONFIG_URI);
        KylinConfig kylinConfig = KylinConfig.createInstanceFromUri(kylinConfigUri);

        int partition = DEFAULT_PARTITION_MONTH;
        if (optionsHelper.getOptionValue(OPTION_PARTITION_MONTH) != null) {
            partition = Integer.valueOf(optionsHelper.getOptionValue(OPTION_PARTITION_MONTH));
        }

        int days = DEFAULT_DAYS;
        if (optionsHelper.getOptionValue(OPTION_DAYS) != null) {
            days = Integer.valueOf(optionsHelper.getOptionValue(OPTION_DAYS));
        }

        int segmentNum = DEFAULT_SEGMENT_NUM;
        if (optionsHelper.getOptionValue(OPTION_SEGMENT_NUM) != null) {
            segmentNum = Integer.valueOf(optionsHelper.getOptionValue(OPTION_SEGMENT_NUM));
        }

        int topNumber = DEFAULT_TOPNUMBER;
        if (optionsHelper.getOptionValue(OPTION_TOPNUMBER) != null) {
            topNumber = Integer.valueOf(optionsHelper.getOptionValue(OPTION_TOPNUMBER));
        }

        int queryDays = DEFAULT_QUERY_DAYS;
        if (optionsHelper.getOptionValue(OPTION_QUERY_DAYS) != null) {
            queryDays = Integer.valueOf(optionsHelper.getOptionValue(OPTION_QUERY_DAYS));
        }

        double percent = DEFAULT_PERCENT;
        if (optionsHelper.getOptionValue(OPTION_PERCENT) != null) {
            percent = Integer.valueOf(optionsHelper.getOptionValue(OPTION_PERCENT));
        }
        String user = optionsHelper.getOptionValue(OPTION_USER);
        String password = optionsHelper.getOptionValue(OPTION_PASSWORD);
        Map<String, Set<String>> tableStats = new LinkedHashMap();

        Set<CubeInstance> cubeSet = getAllCubesByKylinConfig(kylinConfig);
        JDBCDriverCLI jdbcDriverCLI = new JDBCDriverCLI();


        Set<String> cubeNameSet = getCubesByStatus(cubeSet, RealizationStatusEnum.DESCBROKEN);
        tableStats.put("broken cube  (total number: " + cubeNameSet.size() + ")", cubeNameSet);
        cubeNameSet = getCubesByStatus(cubeSet, RealizationStatusEnum.DISABLED);
        tableStats.put("disable cube  (total number: " + cubeNameSet.size() + ")", cubeNameSet);
        cubeNameSet = getNewSegmentsWithoutJob(cubeSet, kylinConfig);
        tableStats.put("cubes which has new segments, but without related jobs  (total number: " + cubeNameSet.size() + ")", cubeNameSet);
        cubeNameSet = getCubesBySegments(cubeSet, segmentNum);
        tableStats.put("cubes which has more than " + segmentNum + " segments  (total number: " + cubeNameSet.size() + ")", cubeNameSet);
        cubeNameSet = lastBuildTimeEarlierNDays(cubeSet, days);
        tableStats.put("cubes whose last build time is earlier than " + days + " days  (total number: " + cubeNameSet.size() + ")", cubeNameSet);
        cubeNameSet = getNotQueriedCubesWithinNDays(jdbcDriverCLI, kylinConfigUri, PROJECTNAME, queryDays, user, password, cubeSet);
        tableStats.put("cubes which are not queried within " + queryDays + " days  (total number: " + cubeNameSet.size() + ")", cubeNameSet);
        cubeNameSet = getCubesByHitRatio(cubeSet, jdbcDriverCLI, kylinConfigUri, PROJECTNAME, user, password, percent);
        tableStats.put("cubes, for which the ratio of built cuboids that never hit is larger than " + percent + "  (total number: " + cubeNameSet.size() + ")", cubeNameSet);
        cubeNameSet = getCubesByQueryPercent(jdbcDriverCLI, kylinConfigUri, PROJECTNAME, user, password);
        tableStats.put("cubes, for which, the 95% of query time cost is more than 1s  (total number: " + cubeNameSet.size() + ")", cubeNameSet);
        cubeNameSet = getCubesCreatedWithinNDays(cubeSet, days);
        tableStats.put(" cubes created within " + days + " days  (total number: " + cubeNameSet.size() + ")", cubeNameSet);
        tableStats.put("top " + topNumber + " queried cubes", getTopQueryCubes(jdbcDriverCLI, kylinConfigUri, PROJECTNAME, topNumber, user, password));


        StringBuilder tableStatsInfo = new StringBuilder();
        if (!tableStats.isEmpty()) {
            tableStatsInfo.append(printHTableStats(tableStats, cubeSet, partition));
        }

        Set<String> receiverSet = Sets.newHashSet(optionsHelper.getOptionValue(OPTION_RECEIVERS).split(","));
        List<String> receivers = Lists.newArrayList(FluentIterable.from(receiverSet).filter(new Predicate<String>() {

            @Override
            public boolean apply(String receiver) {
                return receiver.contains("@");
            }

        }));
        String title = MailTemplateProvider.getMailTitle("ANALYZE_CUBE", "INFORMATION", kylinConfig.getDeployEnv());
        emailHTableStatsInfo(receivers, title, tableStatsInfo.toString());
    }

    private static void emailHTableStatsInfo(List<String> receivers, String subject, String content) {
        new MailService(KylinConfig.getInstanceFromEnv()).sendMail(receivers, subject, content);
    }

    private static String printHTableStats(Map<String, Set<String>> tableStats, Set<CubeInstance> cubeSet, int partitionMonth) {
        Map<String, Object> root = Maps.newHashMap();
        List<Map> partitionList = Lists.newArrayList();
        for (String key : getCubeNumByPartitionMonth(cubeSet, partitionMonth).keySet()) {
            Map<String, Object> kvMap = Maps.newHashMap();
            kvMap.put("key", key);
            kvMap.put("value", getCubeNumByPartitionMonth(cubeSet, partitionMonth).get(key));
            partitionList.add(kvMap);

        }

        root.put("partitionList", partitionList);
        List<Map> cubeTypeList = Lists.newArrayList();
        for (String type : tableStats.keySet()) {
            Map<String, Object> cubeTypeMap = Maps.newHashMap();
            cubeTypeMap.put("cubeType", type);
            List<Map> cubeNameList = Lists.newArrayList();
            for (String cubeName : tableStats.get(type)) {
                if (cubeName != null && !cubeName.equalsIgnoreCase("null")) {
                    Map<String, Object> cubeNameMap = Maps.newHashMap();
                    cubeNameMap.put("cubeName", cubeName);
                    cubeNameList.add(cubeNameMap);
                }
            }
            cubeTypeMap.put("cubeNameList", cubeNameList);
            cubeTypeList.add(cubeTypeMap);
        }
        root.put("cubeTypeList", cubeTypeList);
        String content = MailTemplateProvider.getInstance().buildMailContent("ANALYZE_CUBE_INFO", root);
        return content;
    }

    /**
     * @Description: Get how many new cubes created by partitionMonth
     * @Param: [kylinConfig:system, partitionMonth:we use this param to partition time]
     */
    private static Map<String, Integer> getCubeNumByPartitionMonth(Set<CubeInstance> cubeSet, int partitionMonth) {
        Map<String, Integer> result = Maps.newHashMap();

        for (CubeInstance cubeInstance : cubeSet) {
            String date = stampToDate(cubeInstance.getCreateTimeUTC());
            String year = date.split("-")[0];
            double month = Integer.valueOf(date.split("-")[1]);
            int partitionNum = (int) Math.ceil(month / partitionMonth);
            String quaterkey = year + "-" + partitionNum;
            if (result.containsKey(quaterkey)) {
                result.put(quaterkey, result.get(quaterkey) + 1);
            } else {
                result.put(quaterkey, 1);
            }
        }
        Map<String, Integer> treeMap = new TreeMap<>(result);

        if (treeMap.isEmpty()) {
            logger.warn("Usage: cube is null");
            return Maps.newHashMap();
        }
        for (String key : treeMap.keySet()) {
            logger.trace(key + "build cube number is：" + treeMap.get(key));
        }
        return treeMap;

    }

    /**
     * @Description: Get cubes created within specified days
     * @Param: [kylinConfig:system config, days:this param is defined to partition the time]
     */
    private static Set<String> getCubesCreatedWithinNDays(Set<CubeInstance> cubeSet, int days) {
        Set<String> cubeNameSet = Sets.newHashSet();

        for (CubeInstance cube : cubeSet) {
            long cubeTimeStamp = cube.getCreateTimeUTC();
            if (cubeTimeStamp > nDaysBeforeTimeStamp(days)) {
                cubeNameSet.add(cube.getName());
            }
        }
        logger.trace("Cubes created within {} days: {} ", days, cubeNameSet.toString());
        return cubeNameSet;
    }

    /**
     * @Description: Get cubes whose last build time is earlier the time that we define
     * @Param: [kylinConfig:system config, days:this param is defined to partition the time]
     */
    static Set<String> lastBuildTimeEarlierNDays(Set<CubeInstance> cubeSet, int days) {

        Set<String> resultCubeNameSet = new HashSet<>();
        for (CubeInstance cube : cubeSet) {
            CubeSegment cubeSegment = cube.getLatestBuiltSegment();
            if (cubeSegment != null) {
                long lastBuildTime = cubeSegment.getLastBuildTime();
                if (lastBuildTime < nDaysBeforeTimeStamp(days)) {
                    resultCubeNameSet.add(cube.getName());
                }
            }

        }
        if (cubeSet.isEmpty()) {
            logger.warn("Usage: cubeSet is null");
            return Sets.newHashSet();
        }

        logger.trace("Cubes created within {} days: {}", days, resultCubeNameSet.toString());
        return resultCubeNameSet;

    }
    //    get disable cubes

    /**
     * @Description: get the cubes which status we defined
     * @Param: [kylinConfig:system config, status:cube status]
     */
    private static Set<String> getCubesByStatus(Set<CubeInstance> cubeSet, RealizationStatusEnum status) {
        Set<String> statusCubesSet = new HashSet<>();

        for (CubeInstance cube : cubeSet) {
            RealizationStatusEnum statusEnum = cube.getStatus();
            if (statusEnum.equals(status)) {
                statusCubesSet.add(cube.getName());
            }
        }
        logger.trace("{} cubes:{}", status, statusCubesSet.toString());
        return statusCubesSet;
    }

    /**
     * @Description: Get top 10 queried cubes
     * @Param: [url, projectName, topNumber:the number  show for us from query rest ]
     */
    private static Set<String> getTopQueryCubes(JDBCDriverCLI jdbcDriverCLI, String url, String projectName, int topNumber, String user, String password) throws Exception {
        String sql = "select realization,count(*) as cnt from " + new HiveSink().getTableFromSubject(KylinConfig.createInstanceFromUri(url).getKylinMetricsSubjectQuery()) + " group by realization "
                + "order by cnt desc limit " + topNumber;

        ResultSet resultSet = jdbcDriverCLI.getSQLResult(sql, url, projectName, user, password);
        Set<String> set = new HashSet<>();
        if (resultSet == null) {
            return set;
        }
        while (resultSet.next()) {
            set.add(resultSet.getString(1));
        }
        return set;
    }

    /**
     * @Description: Get cubes which are not queried within the time we defined by month
     * @Param: [url, projectName, month:this param is used to define the time]
     */
    static Set<String> getNotQueriedCubesWithinNDays(JDBCDriverCLI jdbcDriverCLI, String url, String projectName, int days, String user, String password, Set<CubeInstance> cubeSet) throws Exception {
        String date = getLastNDaysStamp(days);
        String sql = "select realization,count(*) as cnt from "
                + new HiveSink().getTableFromSubject(KylinConfig.createInstanceFromUri(url).getKylinMetricsSubjectQuery())
                + " where kday_date >'" + date + "' group by realization";

        ResultSet resultSet = jdbcDriverCLI.getSQLResult(sql, url, projectName, user, password);
        Set<String> queriedCubesWithinNDays = new HashSet<>();
        Set<String> allSet = new HashSet<>();


        for (CubeInstance cube : cubeSet) {
            allSet.add(cube.getName());
        }
        if (resultSet == null) {
            return allSet;
        }
        while (resultSet.next()) {
            queriedCubesWithinNDays.add(resultSet.getString(1));
        }

        for (String cubeName : queriedCubesWithinNDays) {
            allSet.remove(cubeName);
        }
        return allSet;
    }

    /**
     * @Description: Get cubes which has new segments, but without related jobs for the new segments
     * @Param: [kylinConfig]
     */
    private static Set<String> getNewSegmentsWithoutJob(Set<CubeInstance> cubeSet, KylinConfig kylinConfig) {
        Set<String> segmentsWithoutJobSet = new HashSet<>();

        for (CubeInstance cube : cubeSet) {
            for (CubeSegment cubeSegment : cube.getSegments()) {
                if (SegmentStatusEnum.NEW.equals(cubeSegment.getStatus())) {
                    String uuid = cubeSegment.getLastBuildJobID();
                    if (uuid == null) {
                        segmentsWithoutJobSet.add(cube.getName());
                    }
                    ExecutableManager executableManager = ExecutableManager.getInstance(kylinConfig);
                    if (executableManager.getJob(uuid) == null) {
                        segmentsWithoutJobSet.add(cube.getName());
                    }
                }
            }
        }

        logger.trace("cubes which has new segments, but without related jobs for the new segments:{}", segmentsWithoutJobSet.toString());
        return segmentsWithoutJobSet;
    }

    /**
     * @Description: Get cubes which has more than the segment  number that we define
     * @Param: [kylinConfig, num:the segment number]
     */
    private static Set<String> getCubesBySegments(Set<CubeInstance> cubeSet, int num) {
        Set<String> cubesBySegmentsSet = new HashSet<>();

        for (CubeInstance cube : cubeSet) {
            int count = 0;
            for (CubeSegment cubeSegment : cube.getSegments()) {
                count++;
            }
            if (count > num)
                cubesBySegmentsSet.add(cube.getName());
        }

        logger.trace("Get cubes which has more than {} segments:{}", num, cubesBySegmentsSet.toString());
        return cubesBySegmentsSet;
    }

    /**
     * @Description: Get cubes, for which, the 95% of query time cost is more than 1s
     * @Param: [url, projectName]
     */
    private static Set<String> getCubesByQueryPercent(JDBCDriverCLI jdbcDriverCLI, String url, String projectName, String user, String password) throws Exception {
        String sql = "select realization from "
                + new HiveSink().getTableFromSubject(KylinConfig.createInstanceFromUri(url).getKylinMetricsSubjectQuery())
                + " group by realization having percentile(" + new HiveSink().getTableFromSubject(KylinConfig.createInstanceFromUri(url).getKylinMetricsSubjectQuery()) + ".QUERY_TIME_COST,0.05)>1000";


        ResultSet resultSet = jdbcDriverCLI.getSQLResult(sql, url, projectName, user, password);

        Set<String> set = new HashSet<>();
        if (resultSet == null) {
            return set;
        }

        while (resultSet.next()) {
            set.add(resultSet.getString(1));
        }
        return set;
    }

    /**
     * @Description: Get cubes, for which the ratio of built cuboids that never hit is larger than a percent
     * @Param: [kylinConfig, url, projectName, percent]
     */
    private static Set<String> getCubesByHitRatio(Set<CubeInstance> cubeSet, JDBCDriverCLI jdbcDriverCLI, String url, String projectName,
                                                  String user, String password, double percent) throws Exception {
        Set<String> cubesHitRatioSet = new HashSet<>();

        for (CubeInstance cube : cubeSet) {
            Set<Long> cubiosId = cube.getCuboidScheduler().getAllCuboidIds();
            double buildCuboidNum = cubiosId.size();
            double targetNum = 0;
            String sql = "select CUBE_NAME,CUBOID_TARGET from "
                    + new HiveSink().getTableFromSubject(
                    KylinConfig.createInstanceFromUri(url).getKylinMetricsSubjectQueryCube())
                    + " group by CUBE_NAME,CUBOID_TARGET";

            ResultSet resultSet = jdbcDriverCLI.getSQLResult(sql, url, projectName, user, password);
            while (resultSet.next()) {
                targetNum++;
            }
            if (targetNum / buildCuboidNum < (1 - percent)) {
                cubesHitRatioSet.add(cube.getName());
            }
        }
        logger.trace("Get cubes, for which the ratio of built cuboids that never hit is larger than {}: {}", percent, cubesHitRatioSet.toString());
        return cubesHitRatioSet;
    }


    private static String stampToDate(Long stamp) {
        String res = new String();
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM", Locale.ROOT);
        simpleDateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
        Date date = new Date(stamp);
        res = simpleDateFormat.format(date);
        return res;
    }

    static Set<CubeInstance> getAllCubesByKylinConfig(KylinConfig kylinConfig) {
        Set<CubeInstance> cubeSet = new HashSet<>();
        ProjectManager projectManager = ProjectManager.getInstance(kylinConfig);
        CubeManager cubeManager = CubeManager.getInstance(kylinConfig);
        for (ProjectInstance projectInstance : projectManager.listAllProjects()) {
            for (RealizationEntry realizationEntry : projectInstance.getRealizationEntries(RealizationType.CUBE)) {
                CubeInstance cubeInstance = cubeManager.getCube(realizationEntry.getRealization());
                if (cubeInstance == null) {
                    logger.warn("Cannot find cube " + realizationEntry.getRealization());
                    continue;
                }
                cubeSet.add(cubeInstance);

            }
        }
        return cubeSet;
    }

    private static String getLastNDaysStamp(int days) {
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd", Locale.ROOT);
        df.setTimeZone(TimeZone.getTimeZone("GMT"));
        Date date = new Date();
        Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("GMT"), Locale.ROOT);
        calendar.setTime(date);
        calendar.add(Calendar.DATE, -days);
        date = calendar.getTime();
        return df.format(date);
    }

    public static long nDaysBeforeTimeStamp(int days) {
        return System.currentTimeMillis() - 86400000L * days;
    }

}