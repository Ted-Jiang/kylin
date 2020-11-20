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

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
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
import org.apache.kylin.metadata.model.IStorageAware;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.project.ProjectManager;
import org.apache.kylin.metadata.project.RealizationEntry;
import org.apache.kylin.metadata.realization.RealizationType;
import org.apache.kylin.metrics.MetricsManager;
import org.apache.kylin.metrics.lib.impl.hive.HiveSink;
import org.apache.kylin.rest.response.HBaseResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;

public class CubeRetireDetection {

    private static final Logger logger = LoggerFactory.getLogger(CubeRetireDetection.class);
    private static final int DEFAULT_DAYS = 30;
    private static final int RETIRED_DAYS = 7;
    private static final String PROJECT_NAME = MetricsManager.SYSTEM_PROJECT;
    private static final String NO_BUILD = "Never Build";
    private static final String NO_QUERY = "Never Query";
    private static final String NO_OWNER = "N/A";

    @SuppressWarnings("static-access")
    private static final Option OPTION_KYLIN_URI_BASE = OptionBuilder.withArgName("kylinURIBase").hasArg()
            .isRequired(true).withDescription("Base Kylin URI for finding related cubes").create("kylinURIBase");

    @SuppressWarnings("static-access")
    private static final Option OPTION_KYLIN_URI_ADDITIONAL = OptionBuilder.withArgName("kylinURIAdditional").hasArg()
            .isRequired(false).withDescription("Additional Kylin URI for finding related cubes").create("kylinURIAdditional");

    @SuppressWarnings("static-access")
    private static final Option OPTION_USER = OptionBuilder.withArgName("user").hasArg()
            .isRequired(true).withDescription("User for authentication").create("user");

    @SuppressWarnings("static-access")
    private static final Option OPTION_PASSWORD = OptionBuilder.withArgName("password").hasArg()
            .isRequired(true).withDescription("Password for authentication").create("password");

    @SuppressWarnings("static-access")
    private static final Option OPTION_RECEIVERS = OptionBuilder.withArgName("receivers").hasArg()
            .isRequired(false).withDescription("email address for receivers").create("receivers");

    @SuppressWarnings("static-access")
    private static final Option OPTION_BUILD_DAYS = OptionBuilder.withArgName("buildDays").hasArg()
            .isRequired(false).withDescription("Get cubes which is not build within the time").create("buildDays");

    @SuppressWarnings("static-access")
    private static final Option OPTION_QUERY_DAYS = OptionBuilder.withArgName("queryDays").hasArg()
            .isRequired(false).withDescription("Get cubes which is not queried within the time").create("queryDays");

    @SuppressWarnings("static-access")
    private static final Option OPTION_RETIRED_DAYS =  OptionBuilder.withArgName("retiredDays").hasArg()
            .isRequired(false).withDescription("Cube will be retired within the time").create("retiredDays");

    @SuppressWarnings("static-access")
    private static final Option OPTION_DRY_RUN =  OptionBuilder.withArgName("dryRun").hasArg()
            .isRequired(false).withDescription("Only send summary mail").create("dryRun");


    public static void main(String[] args) throws Exception {

        Options options = new Options();
        OptionsHelper optionsHelper = new OptionsHelper();
        options.addOption(OPTION_KYLIN_URI_BASE);
        options.addOption(OPTION_KYLIN_URI_ADDITIONAL);
        options.addOption(OPTION_USER);
        options.addOption(OPTION_PASSWORD);
        options.addOption(OPTION_RECEIVERS);
        options.addOption(OPTION_QUERY_DAYS);
        options.addOption(OPTION_BUILD_DAYS);
        options.addOption(OPTION_RETIRED_DAYS);
        options.addOption(OPTION_DRY_RUN);

        try {
            optionsHelper.parseOptions(options, args);
        } catch (ParseException e) {
            e.printStackTrace();
        }

        String baseURI = optionsHelper.getOptionValue(OPTION_KYLIN_URI_BASE);
        String user = optionsHelper.getOptionValue(OPTION_USER);
        String password = optionsHelper.getOptionValue(OPTION_PASSWORD);

        int buildDays = DEFAULT_DAYS;
        if (optionsHelper.getOptionValue(OPTION_BUILD_DAYS) != null) {
            buildDays = Integer.valueOf(optionsHelper.getOptionValue(OPTION_BUILD_DAYS));
        }

        int queryDays = DEFAULT_DAYS;
        if (optionsHelper.getOptionValue(OPTION_QUERY_DAYS) != null) {
            queryDays = Integer.valueOf(optionsHelper.getOptionValue(OPTION_QUERY_DAYS));
        }

        int retiredDays = RETIRED_DAYS;
        if (optionsHelper.getOptionValue(OPTION_RETIRED_DAYS) != null) {
            retiredDays = Integer.valueOf(optionsHelper.getOptionValue(OPTION_RETIRED_DAYS));
        }

        List<String> receivers = Lists.newArrayList();
        if (optionsHelper.getOptionValue(OPTION_RECEIVERS) != null) {
            receivers = Lists.newArrayList(optionsHelper.getOptionValue(OPTION_RECEIVERS).split(","));
        }

        boolean dryRun = true;
        if (optionsHelper.getOptionValue(OPTION_DRY_RUN) != null) {
            dryRun = Boolean.valueOf(optionsHelper.getOptionValue(OPTION_DRY_RUN));
        }

        JDBCDriverCLI jdbcDriverCLI = new JDBCDriverCLI();

        KylinConfig kylinConfig = KylinConfig.
                createInstanceFromUri(baseURI);
        Map<String, Set<CubeInstance>> allCubes = getAllCubesByKylinConfig(kylinConfig);

        Set<String> additionalCubes = Sets.newHashSet();

        if (optionsHelper.getOptionValue(OPTION_KYLIN_URI_ADDITIONAL) != null) {
            KylinConfig kylinConfigAdditional = KylinConfig.createInstanceFromUri(optionsHelper.getOptionValue(OPTION_KYLIN_URI_ADDITIONAL));
            additionalCubes = Sets.newHashSet(FluentIterable
                    .from(AnalyzeCube.getAllCubesByKylinConfig(kylinConfigAdditional))
                    .transform(new Function<CubeInstance, String>() {
                        @Override
                        public String apply(CubeInstance cube) {
                            return cube.getName();
                        }
                    }));
        }


        final Set<String> cubeNoBuild = getNoBuildCubes(allCubes, buildDays);

        final Map<String, String> cubeQueryInfo = getCubeQueryDate(jdbcDriverCLI, baseURI, user, password);

        final Set<String> cubeInactive = getInactiveCubes(cubeNoBuild, cubeQueryInfo, queryDays);

        final Set<String> includeCubes = additionalCubes;

        Map<String, Set<CubeInstance>> retireCubes = Maps.newHashMap();
        for (String project: allCubes.keySet()) {
            Set<CubeInstance> cubes = Sets.newHashSet(Iterables.filter(allCubes.get(project), new Predicate<CubeInstance>() {
                @Override
                public boolean apply(@Nullable CubeInstance cube) {
                    return cube != null && cubeInactive != null && cubeInactive.contains(cube.getName()) && (includeCubes == null || !includeCubes.contains(cube.getName()));
                }
            }));
            if (!cubes.isEmpty()) {
                retireCubes.put(project, cubes);
            }
        }

        if (!dryRun) {
            sendMailToUser(retireCubes, baseURI, retiredDays, receivers, Math.min(buildDays, queryDays), cubeQueryInfo);
        }
        sendMailToAdmin(retireCubes, baseURI, retiredDays, receivers, cubeQueryInfo);
    }

    private static void sendMailToUser(Map<String, Set<CubeInstance>> retiredCubes, String uri, int retiredDays, List<String> receivers, int inactiveDays, Map<String, String> cubeQueryInfo) {
        KylinConfig kylinConfig = KylinConfig.createInstanceFromUri(uri);
        String env = kylinConfig.getDeployEnv();
        for (String project : retiredCubes.keySet()) {
            Set<CubeInstance> cubes = retiredCubes.get(project);
            for (CubeInstance cube : cubes) {
                String title = MailTemplateProvider.getMailTitle("RETIRED CUBE", "WARN", env, project, cube.getName());
                List<String> users = Lists.newArrayList();
                if (receivers.isEmpty()) {
                    users.addAll(cube.getDescriptor().getNotifyList());
                    final String[] adminDls = kylinConfig.getAdminDls();
                    if (null != adminDls) {
                        for (String adminDl : adminDls) {
                            users.add(adminDl);
                        }
                    }
                } else {
                    users = receivers;
                }

                String lastBuildDate = cube.getLatestBuiltSegment() == null ? NO_BUILD : getDate(cube.getLatestBuiltSegment().getLastBuildTime(), Integer.MIN_VALUE);
                String lastQueryDate = cubeQueryInfo.get(cube.getName());
                Map<String, Object> dataMap = Maps.newHashMap();
                dataMap.put("retired_day", retiredDays);
                dataMap.put("env",  env);
                dataMap.put("inactive_days", inactiveDays);
                dataMap.put("cube_name",  cube.getName());
                dataMap.put("retired_date",  getDate(Long.MIN_VALUE, retiredDays));
                dataMap.put("last_build_date", lastBuildDate);
                dataMap.put("last_query_date", lastQueryDate == null ? NO_QUERY : lastQueryDate);
                String content = MailTemplateProvider.getInstance().buildMailContent("CUBE_RETIRE_DETECTION",
                        dataMap);
                new MailService(KylinConfig.getInstanceFromEnv()).sendMail(users, title, content);
            }
        }
    }

    private static  void sendMailToAdmin(Map<String, Set<CubeInstance>> retiredCubes, String uri, int retiredDays, List<String> receivers, Map<String, String> cubeQueryInfo) {
        KylinConfig kylinConfig = KylinConfig.createInstanceFromUri(uri);
        String env = kylinConfig.getDeployEnv();
        String title = MailTemplateProvider.getMailTitle("RETIRED CUBE SUMMARY", "INFO", env);
        List<String> users = Lists.newArrayList();
        if (receivers.isEmpty()) {
            final String[] adminDls = kylinConfig.getAdminDls();
            if (null != adminDls) {
                for (String adminDl : adminDls) {
                    users.add(adminDl);
                }
            }
        } else {
            users = receivers;
        }
        boolean needClean = retiredCubes.isEmpty() ? false : true;
        List<Map<String, String>> cubes = Lists.newArrayList();
        for (String project : retiredCubes.keySet()) {
            Set<CubeInstance> cubesSet = retiredCubes.get(project);
            for (CubeInstance cube : cubesSet) {
                String lastBuildDate = cube.getLatestBuiltSegment() == null ? NO_BUILD
                        : getDate(cube.getLatestBuiltSegment().getLastBuildTime(), Integer.MIN_VALUE);
                String lastQueryDate = cubeQueryInfo.get(cube.getName());
                String cubeOwner = cube.getOwner();
                String cubeSizeGB = String.format(Locale.ROOT, "%.2f", (cube.getSizeKB() / 1024.0 / 1024));
                int regionCount = 0;
                for (CubeSegment segment : cube.getSegments()) {
                    String tableName = segment.getStorageLocationIdentifier();
                    HBaseResponse hr = null;
                    try {
                        if (cube.getStorageType() == IStorageAware.ID_HBASE
                                || cube.getStorageType() == IStorageAware.ID_SHARDED_HBASE
                                || cube.getStorageType() == IStorageAware.ID_REALTIME_AND_HBASE) {
                            try {
                                logger.debug("Loading HTable info " + cube.getName() + ", " + tableName);

                                hr = (HBaseResponse) Class.forName("org.apache.kylin.rest.service.HBaseInfoUtil")
                                        .getMethod("getHBaseInfo", new Class[] { String.class, KylinConfig.class })
                                        .invoke(null, tableName, kylinConfig);
                                regionCount += hr.getRegionCount();
                            } catch (Throwable e) {
                                throw new IOException(e);
                            }
                        }

                    } catch (IOException e) {
                        logger.error("Failed to calcuate size of HTable \"" + tableName + "\".", e);
                    }
                }
                String cubeRegionCount = String.valueOf(regionCount);

                Map<String, String> cubeInfo = Maps.newHashMap();
                cubeInfo.put("name", cube.getName());
                cubeInfo.put("project", project);
                cubeInfo.put("last_build_date", lastBuildDate);
                cubeInfo.put("last_query_date", lastQueryDate == null ? NO_QUERY : lastQueryDate);
                cubeInfo.put("cube_owner", cubeOwner == null ? NO_OWNER : cubeOwner);
                cubeInfo.put("cube_sizeGB", cubeSizeGB);
                cubeInfo.put("region_count", cubeRegionCount);
                cubes.add(cubeInfo);
            }
        }
        cubes.sort((a, b) -> Double.compare(Double.parseDouble(b.get("cube_sizeGB")), Double.parseDouble(a.get("cube_sizeGB"))));
        cubes.sort((a, b) -> Integer.compare(Integer.parseInt(b.get("region_count")),
                Integer.parseInt(a.get("region_count"))));
        cubes.sort((a, b) -> b.get("project").compareTo(a.get("project")));
        Map<String, Object> dataMap = Maps.newHashMap();
        dataMap.put("cube_num", cubes.size());
        dataMap.put("need_clean", needClean);
        dataMap.put("env",  env);
        dataMap.put("retired_date",  getDate(Long.MIN_VALUE, retiredDays));
        dataMap.put("cubes", cubes);
        String content = MailTemplateProvider.getInstance().buildMailContent("CUBE_RETIRE_SUMMARY",
                dataMap);
        new MailService(KylinConfig.getInstanceFromEnv()).sendMail(users, title, content);
    }

    private static String getDate(long baseTime, int diffDays) {
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd", Locale.ROOT);
        simpleDateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
        Date date = baseTime == Long.MIN_VALUE ? new Date() : new Date(baseTime);
        Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("GMT"), Locale.ROOT);
        calendar.setTime(date);
        if (diffDays != Integer.MIN_VALUE) {
            calendar.add(Calendar.DATE, diffDays);
        }
        date = calendar.getTime();
        return simpleDateFormat.format(date);
    }

    private static Map<String, String> getCubeQueryDate(JDBCDriverCLI jdbcDriverCLI, String url, String user, String password) throws SQLException {
        String sql = "select realization, max(kday_date) as query_day from "
                + new HiveSink().getTableFromSubject(KylinConfig.createInstanceFromUri(url).getKylinMetricsSubjectQuery())
                + " group by realization";

        ResultSet resultSet = jdbcDriverCLI.getSQLResult(sql, url, PROJECT_NAME, user, password);
        Map<String, String> queryCube = Maps.newHashMap();
        if (resultSet != null) {
            while (resultSet.next()) {
                queryCube.put(resultSet.getString(1), resultSet.getString(2));
            }
        }

        return queryCube;
    }

    private static Set<String> getInactiveCubes(Set<String> noBuildCubes, Map<String, String> queryCubeInfo, int queryDays) throws Exception{
        Set<String> inactiveCube = Sets.newHashSet();
        Date queryBefore = new SimpleDateFormat("yyyy-MM-dd", Locale.ROOT).parse(getDate(Long.MIN_VALUE, -queryDays));
        for(String cubeName: noBuildCubes) {
            if (queryCubeInfo.get(cubeName) == null) {
                inactiveCube.add(cubeName);
            } else {
                Date queryDate = new SimpleDateFormat("yyyy-MM-dd", Locale.ROOT).parse(queryCubeInfo.get(cubeName));
                if (queryDate.compareTo(queryBefore) <= 0) {
                    inactiveCube.add(cubeName);
                }
            }
        }
        return inactiveCube;
    }

    private static Map<String, Set<CubeInstance>> getAllCubesByKylinConfig(KylinConfig kylinConfig) {
        Map<String, Set<CubeInstance>> allCubes = Maps.newHashMap();
        ProjectManager projectManager = ProjectManager.getInstance(kylinConfig);
        CubeManager cubeManager = CubeManager.getInstance(kylinConfig);
        for (ProjectInstance projectInstance : projectManager.listAllProjects()) {
            Set<CubeInstance> cubes = Sets.newHashSet();
            for (RealizationEntry realizationEntry : projectInstance.getRealizationEntries(RealizationType.CUBE)) {
                CubeInstance cubeInstance = cubeManager.getCube(realizationEntry.getRealization());
                if (cubeInstance == null) {
                    logger.warn("Cannot find cube " + realizationEntry.getRealization());
                    continue;
                }
                cubes.add(cubeInstance);
            }
            if (!cubes.isEmpty()) {
                allCubes.put(projectInstance.getName(), cubes);
            }
        }
        return allCubes;
    }

    private static Set<String> getNoBuildCubes(Map<String, Set<CubeInstance>> allCubes, int buildDays) {
        Set<CubeInstance> allCubesSet = Sets.newHashSet();
        for(String project : allCubes.keySet()) {
            Set<CubeInstance> cubes = allCubes.get(project);
            if (!cubes.isEmpty()) {
                allCubesSet.addAll(cubes);
            }
        }
        return AnalyzeCube.lastBuildTimeEarlierNDays(allCubesSet, buildDays);
    }

}