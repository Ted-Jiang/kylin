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

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.hadoop.hbase.client.Connection;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.MailService;
import org.apache.kylin.common.util.MailTemplateProvider;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.project.ProjectManager;
import org.apache.kylin.metadata.project.RealizationEntry;
import org.apache.kylin.metadata.realization.RealizationType;
import org.apache.kylin.shaded.com.google.common.collect.Lists;
import org.apache.kylin.shaded.com.google.common.collect.Maps;
import org.apache.kylin.shaded.com.google.common.collect.Sets;
import org.apache.kylin.shaded.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.kylin.storage.hbase.HBaseConnection;
import org.apache.kylin.storage.hbase.util.HBaseRegionSizeCalculator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HTableMonitorCLI {
    private static final Logger logger = LoggerFactory.getLogger(HTableMonitorCLI.class);

    private static final int THREAD_NUM = 10;

    public static void main(String[] args) throws IOException {

        if (args.length != 2) {
            logger.warn("Usage: HTableMonitorCLI receivers kylinConfigUris");
            return;
        }

        StringBuilder htableStatsInfo = new StringBuilder();
        Map<String, Map<String, Map<String, Map<String, HTableStats>>>> hTableStats = Maps.newHashMap();
        for (String kylinConfigUri : args[1].split(",")) {
            try {
                hTableStats.put(kylinConfigUri, getHTableStatsByUri(kylinConfigUri));
            } catch (Exception e) {
                String errMsg = "Environment " + kylinConfigUri + ": fail to get htable stats due to " + e;
                logger.warn(errMsg);
                htableStatsInfo.append(errMsg + "\n");
            }
        }

        if (!hTableStats.isEmpty()) {
            htableStatsInfo.append(printHTableStats(hTableStats));
        }

        Set<String> receiverSet = Sets.newHashSet(args[0].split(","));
        List<String> receivers = receiverSet.stream().filter(r -> r.contains("@")).collect(Collectors.toList());

        String title = MailTemplateProvider.getMailTitle("HBASE", "USAGE_CHECK");
        emailHTableStatsInfo(receivers, title, htableStatsInfo.toString());
    }

    private static Map<String, Map<String, Map<String, HTableStats>>> getHTableStatsByUri(String kylinConfigUri)
            throws IOException {

        Map<String, Map<String, Map<String, HTableStats>>> result = Maps.newHashMap();

        KylinConfig kylinConfig = KylinConfig.createInstanceFromUri(kylinConfigUri);
        final Connection conn = HBaseConnection.get(kylinConfig.getStorageUrl());
        ProjectManager projectManager = ProjectManager.getInstance(kylinConfig);
        CubeManager cubeManager = CubeManager.getInstance(kylinConfig);
        ExecutorService executor = Executors.newFixedThreadPool(THREAD_NUM,
                new ThreadFactoryBuilder().setNameFormat("hbase-usage-check-pool-%d").build());

        for (ProjectInstance projectInstance : projectManager.listAllProjects()) {
            Map<String, Map<String, HTableStats>> projRet = Maps.newHashMap();
            result.put(projectInstance.getName(), projRet);

            for (RealizationEntry realizationEntry : projectInstance.getRealizationEntries(RealizationType.CUBE)) {
                CubeInstance cubeInstance = cubeManager.getCube(realizationEntry.getRealization());
                if (cubeInstance == null) {
                    logger.warn("Cannot find cube " + realizationEntry.getRealization());
                    continue;
                }

                final Map<String, HTableStats> cubeRet = Maps.newConcurrentMap();
                projRet.put(cubeInstance.getName(), cubeRet);

                for (final CubeSegment cubeSegment : cubeInstance.getSegments(SegmentStatusEnum.READY)) {
                    executor.submit(new Runnable() {
                        @Override
                        public void run() {
                            String hTableName = cubeSegment.getStorageLocationIdentifier();
                            try {
                                HBaseRegionSizeCalculator cal = new HBaseRegionSizeCalculator(hTableName, conn);//the regions info in a table
                                long tableSize = 0L;
                                Map<byte[], Long> sizeMap = cal.getRegionSizeMap();
                                for (long s : sizeMap.values()) {
                                    tableSize += s;
                                }
                                cubeRet.put(hTableName, new HTableStats(1, sizeMap.size(), tableSize));
                            } catch (IOException e) {
                                logger.error(e.getMessage());
                            }
                        }
                    });
                }
            }
        }

        executor.shutdown();
        try {
            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            logger.error(e.getMessage());
        }

        return result;
    }

    private static class HTableStats {
        int tableCount = 0;
        int regionCount = 0;
        long tableSize = 0L;

        HTableStats(int tableCount, int regionCount, long tableSize) {
            this.tableCount = tableCount;
            this.regionCount = regionCount;
            this.tableSize = tableSize;
        }

        public String toString() {
            return "(" + regionCount + " regions - " + tableCount + " htables - " + calculateSize(tableSize) + ")";
        }
    }

    public static String calculateSize(long size) {
        if (size < 0) {
            return "NA";
        }
        if ((size >> 60) > 0) {
            return String.format(Locale.ROOT, "%.2fE", size / Math.pow(2, 60));
        }
        if ((size >> 50) > 0) {
            return String.format(Locale.ROOT, "%.2fP", size / Math.pow(2, 50));
        }
        if ((size >> 40) > 0) {
            return String.format(Locale.ROOT, "%.2fT", size / Math.pow(2, 40));
        }
        if ((size >> 30) > 0) {
            return String.format(Locale.ROOT, "%.2fG", size / Math.pow(2, 30));
        }
        if ((size >> 20) > 0) {
            return String.format(Locale.ROOT, "%.2fM", size / Math.pow(2, 20));
        }
        if ((size >> 10) > 0) {
            return String.format(Locale.ROOT, "%.2fK", size / Math.pow(2, 10));
        }
        return size + "B";
    }

    private static HTableStats getSummarizedHTableStats(Map<String, HTableStats> hTableStatsMap) {
        int tableCount = 0;
        int regionCount = 0;
        long tableSize = 0L;
        for (HTableStats entry : hTableStatsMap.values()) {
            tableCount += entry.tableCount;
            regionCount += entry.regionCount;
            tableSize += entry.tableSize;
        }
        return new HTableStats(tableCount, regionCount, tableSize);
    }

    private static List<Map.Entry<String, HTableStats>> sortMapEntries(Map<String, HTableStats> map) {
        List<Map.Entry<String, HTableStats>> result = Lists.newArrayList(map.entrySet());
        Collections.sort(result, new Comparator<Map.Entry<String, HTableStats>>() {
            @Override
            public int compare(Map.Entry<String, HTableStats> o1, Map.Entry<String, HTableStats> o2) {
                int ret = o2.getValue().regionCount - o1.getValue().regionCount;
                if (ret == 0) {
                    ret = o2.getValue().tableCount - o1.getValue().tableCount;
                    if (ret == 0) {
                        if (o2.getValue().tableSize > o1.getValue().tableSize) {
                            ret = 1;
                        } else if (o2.getValue().tableSize < o1.getValue().tableSize) {
                            ret = -1;
                        }
                    }
                }
                return ret;
            }
        });
        return result;
    }

    private static String printHTableStats(
            Map<String, Map<String, Map<String, Map<String, HTableStats>>>> hTableStats) {

        Map<String, Map<String, Map<String, HTableStats>>> cubeRet = Maps.newHashMap();
        for (Map.Entry<String, Map<String, Map<String, Map<String, HTableStats>>>> envEntry : hTableStats.entrySet()) {
            Map<String, Map<String, HTableStats>> cubeRetEnv = Maps.newHashMap();
            cubeRet.put(envEntry.getKey(), cubeRetEnv);
            for (Map.Entry<String, Map<String, Map<String, HTableStats>>> projEntry : envEntry.getValue().entrySet()) {
                Map<String, HTableStats> cubeRetProj = Maps.newHashMap();
                cubeRetEnv.put(projEntry.getKey(), cubeRetProj);
                for (Map.Entry<String, Map<String, HTableStats>> cubeEntry : projEntry.getValue().entrySet()) {
                    cubeRetProj.put(cubeEntry.getKey(), getSummarizedHTableStats(cubeEntry.getValue()));
                }
            }
        }

        Map<String, Map<String, HTableStats>> projRet = Maps.newHashMap();
        for (Map.Entry<String, Map<String, Map<String, HTableStats>>> envEntry : cubeRet.entrySet()) {
            Map<String, HTableStats> projRetEnv = Maps.newHashMap();
            projRet.put(envEntry.getKey(), projRetEnv);
            for (Map.Entry<String, Map<String, HTableStats>> projEntry : envEntry.getValue().entrySet()) {
                projRetEnv.put(projEntry.getKey(), getSummarizedHTableStats(projEntry.getValue()));
            }
        }

        Map<String, HTableStats> envRet = Maps.newHashMap();
        for (Map.Entry<String, Map<String, HTableStats>> envEntry : projRet.entrySet()) {
            envRet.put(envEntry.getKey(), getSummarizedHTableStats(envEntry.getValue()));
        }

        Map<String, Object> root = Maps.newHashMap();

        List<Map> envRetList = Lists.newArrayList();
        for (Map.Entry<String, HTableStats> envEntry : sortMapEntries(envRet)) {
            Map<String, Object> envMap = Maps.newHashMap();
            envMap.put("env", envEntry.getKey().toString());
            envMap.put("size", projRet.get(envEntry.getKey()).size());
            envMap.put("regionCount", envEntry.getValue().regionCount);
            envMap.put("tableCount", envEntry.getValue().tableCount);
            envMap.put("tableSize", calculateSize(envEntry.getValue().tableSize));
            envRetList.add(envMap);
        }
        root.put("envRetList", envRetList);

        List<Map> envList = Lists.newArrayList();
        for (Map.Entry<String, HTableStats> envEntry : sortMapEntries(envRet)) {
            Map<String, Object> envMap = Maps.newHashMap();
            envMap.put("env", envEntry.getKey().toString());
            envMap.put("size", projRet.get(envEntry.getKey()).size());
            envMap.put("regionCount", envEntry.getValue().regionCount);
            envMap.put("tableCount", envEntry.getValue().tableCount);
            envMap.put("tableSize", calculateSize(envEntry.getValue().tableSize));
            List<Map> projList = Lists.newArrayList();
            for (Map.Entry<String, HTableStats> projEntry : sortMapEntries(projRet.get(envEntry.getKey()))) {
                Map<String, Object> projMap = Maps.newHashMap();
                projMap.put("proj", projEntry.getKey().toString());
                projMap.put("size", cubeRet.get(envEntry.getKey()).get(projEntry.getKey()).size());
                projMap.put("regionCount", projEntry.getValue().regionCount);
                projMap.put("tableCount", projEntry.getValue().tableCount);
                projMap.put("tableSize", calculateSize(projEntry.getValue().tableSize));
                List<Map> cubeList = Lists.newArrayList();
                for (Map.Entry<String, HTableStats> cubeEntry : sortMapEntries(
                        cubeRet.get(envEntry.getKey()).get(projEntry.getKey()))) {
                    Map<String, Object> cubeMap = Maps.newHashMap();
                    cubeMap.put("cube", cubeEntry.getKey().toString());
                    cubeMap.put("regionCount", cubeEntry.getValue().regionCount);
                    cubeMap.put("tableCount", cubeEntry.getValue().tableCount);
                    cubeMap.put("tableSize", calculateSize(cubeEntry.getValue().tableSize));
                    cubeList.add(cubeMap);
                }
                projMap.put("cubeList", cubeList);
                projList.add(projMap);
            }
            envMap.put("projList", projList);
            envList.add(envMap);
        }
        root.put("envList", envList);

        String content = MailTemplateProvider.getInstance().buildMailContent("HBASE_USAGE_CHECK", root);

        return content;
    }

    private static void emailHTableStatsInfo(List<String> receivers, String subject, String content) {
        new MailService(KylinConfig.getInstanceFromEnv()).sendMail(receivers, subject, content);
    }
}
