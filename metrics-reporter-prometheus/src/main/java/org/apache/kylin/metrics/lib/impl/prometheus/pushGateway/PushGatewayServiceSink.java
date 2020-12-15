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

package org.apache.kylin.metrics.lib.impl.prometheus.pushGateway;

import io.prometheus.client.CollectorRegistry;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.MailService;
import org.apache.kylin.common.util.StringUtil;
import org.apache.kylin.common.util.ToolUtil;
import org.apache.kylin.dict.DictionaryManager;
import org.apache.kylin.dict.lookup.SnapshotManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.prometheus.client.exporter.PushGateway;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class PushGatewayServiceSink {

    private static final Logger log = LoggerFactory.getLogger(PushGatewayServiceSink.class);
    private static  final PushGatewayServiceSink instance = new PushGatewayServiceSink();
    private KylinConfig config;
    private PushGateway pushGateway;


    public PushGatewayServiceSink getInstance(){
        return instance;
    }

    /**
     * This method is called by Spring Framework at kylinMetrics.xml
     */
    public static void initPushGateWay() {
        if(KylinConfig.getInstanceFromEnv().isKylinMetricsPushGatewayEnabled()) {
            instance.init();
            log.info("pushGateWay inited!");
            instance.start();
            log.info("pushGateWay started!");
        }else {
            log.info("pushGateWay in not enable in conf !");
        }
    }

    public void init() {
        config = KylinConfig.getInstanceFromEnv();
        try {
            String endpoint = config.getKylinMetricsPushGatewayEndpoint();
            if (StringUtil.isEmpty(endpoint)) {
                throw new Exception("pushGateWay endpoint not configured");
            }
            pushGateway = new PushGateway(endpoint);
            GatewayMetrics.initialize();
        } catch (Exception e) {
            log.error("Failed to initialize pushGateWay", e);
        }
    }

    public void start() {
        if (pushGateway == null) {
            log.error("Failed to start pushGateWay , gateway is null");
            return;
        }
        Map<String, String> groupingKey = new HashMap<>();
        groupingKey.put("env", "kylin-gateway-" + config.getDeployEnv());
        groupingKey.put("instance", ToolUtil.getHostName());
        Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(() -> {
            try {
                log.info("Start produce pushGateWay metric");
                ThreadInfo[] allThreads = ManagementFactory.getThreadMXBean().dumpAllThreads(false, false);
                int usedTomcatThreadCnt = 0;
                int usedQueryThreadCnt = 0;
                int usedCoprocessorThreadCnt = 0;
                int totalThreadCnt = 0;
                int ipcThreadCnt = 0;
                int sharedHConnThreadCnt = 0;
                int loadSnapShotTableCnt = 0;
                int jobSchedulerCnt = 0;
                int hBaseTableThreadCnt = 0;
                int port7070ThreadCnt = 0;
                int port7443ThreadCnt = 0;
                int port7070ThreadCntAcceptor = 0;
                int port7443ThreadCntAcceptor = 0;

                for (ThreadInfo threadInfo : allThreads) {
                    totalThreadCnt++;
                    String threadName = threadInfo.getThreadName();
                    if (threadName == null) {
                        continue;
                    }
                    if (threadName.startsWith("http-bio-7070-Acceptor")) {
                        port7070ThreadCntAcceptor++;
                        continue;
                    }
                    if (threadName.startsWith("http-bio-7443-Acceptor")) {
                        port7443ThreadCntAcceptor++;
                        continue;
                    }
                    if (threadName.startsWith("http-bio-7070")) {
                        port7070ThreadCnt++;
                        continue;
                    }
                    if (threadName.startsWith("http-bio-7443")) {
                        port7443ThreadCnt++;
                        continue;
                    }
                    if (isQueryThread(threadName)) {
                        usedQueryThreadCnt++;
                        usedTomcatThreadCnt++;
                        continue;
                    }
                    if (threadName.startsWith("http-bio-") && threadName.contains("exec")) {
                        if (isThreadRunningKylinCode(threadInfo)) {
                            usedTomcatThreadCnt++;
                        }
                        continue;
                    }
                    if (threadName.startsWith("kylin-coproc-") && isThreadRunningKylinCode(threadInfo)) {
                        usedCoprocessorThreadCnt++;
                        continue;
                    }
                    if (threadName.startsWith("IPC Client")) {
                        ipcThreadCnt++;
                        continue;
                    }
                    if (threadName.startsWith("hconnection-")) {
                        sharedHConnThreadCnt++;
                        continue;
                    }
                    if (threadName.startsWith("kylin-loading-lookup")) {
                        loadSnapShotTableCnt++;
                        continue;
                    }
                    if (threadName.startsWith("Scheduler")) {
                        jobSchedulerCnt++;
                        continue;
                    }
                    if (threadName.contains("metaLookup")) {
                        hBaseTableThreadCnt++;
                    }
                }

                //thread
                GatewayMetrics.setQueryEngineThreadGauge(GatewayMetrics.ThreadUser.TOMCAT,
                        GatewayMetrics.ThreadStatus.CORE_STA, usedTomcatThreadCnt);
                GatewayMetrics.setQueryEngineThreadGauge(GatewayMetrics.ThreadUser.HBASE_PRC,
                        GatewayMetrics.ThreadStatus.CORE_STA, usedCoprocessorThreadCnt);
                GatewayMetrics.setQueryEngineThreadGauge(GatewayMetrics.ThreadUser.HBASE_IPC,
                        GatewayMetrics.ThreadStatus.CORE_STA, ipcThreadCnt);
                GatewayMetrics.setQueryEngineThreadGauge(GatewayMetrics.ThreadUser.LOAD_LOOKUP_SNAPSHOT,
                        GatewayMetrics.ThreadStatus.CORE_STA, loadSnapShotTableCnt);
                GatewayMetrics.setJobEngineThreadGauge(GatewayMetrics.ThreadUser.JOB_SCHEDULER,
                        GatewayMetrics.ThreadStatus.CORE_STA, jobSchedulerCnt);
                GatewayMetrics.setAllEngineThreadGauge(GatewayMetrics.ThreadUser.HBASE_OPERATION,
                        GatewayMetrics.ThreadStatus.CORE_STA, hBaseTableThreadCnt);
                GatewayMetrics.setAllEngineThreadGauge("all-thread", GatewayMetrics.ThreadStatus.CORE_STA,
                        totalThreadCnt);

                // Dictionary cache metric
                DictionaryManager dictionaryManager = DictionaryManager.getInstance(KylinConfig.getInstanceFromEnv());
                GatewayMetrics.setCacheGauge(GatewayMetrics.CacheCategory.DICT_CACHE, GatewayMetrics.CacheStatus.SIZE,
                        dictionaryManager.getDictCacheSizeMB());
                GatewayMetrics.setCacheGauge(GatewayMetrics.CacheCategory.DICT_CACHE, GatewayMetrics.CacheStatus.NUM,
                        dictionaryManager.getDictCacheNum());
                GatewayMetrics.setCacheGauge(GatewayMetrics.CacheCategory.DICT_CACHE, GatewayMetrics.CacheStatus.EVICT,
                        dictionaryManager.getEvictNumber());

                //Lookup table snapshot cache metric
                SnapshotManager snapshotManager = SnapshotManager.getInstance(KylinConfig.getInstanceFromEnv());
                GatewayMetrics.setCacheGauge(GatewayMetrics.CacheCategory.LOOKUP_CACHE, GatewayMetrics.CacheStatus.SIZE,
                        snapshotManager.getDictCacheSizeMB());
                GatewayMetrics.setCacheGauge(GatewayMetrics.CacheCategory.LOOKUP_CACHE, GatewayMetrics.CacheStatus.NUM,
                        snapshotManager.getDictCacheNum());
                GatewayMetrics.setCacheGauge(GatewayMetrics.CacheCategory.LOOKUP_CACHE, GatewayMetrics.CacheStatus.EVICT,
                        snapshotManager.getEvictNumber());

                //7070 & 7443 port thread
                GatewayMetrics.setAllEngineThreadGauge(GatewayMetrics.ThreadUser.PORT_7070,
                        GatewayMetrics.ThreadStatus.CORE_STA, port7070ThreadCnt);
                GatewayMetrics.setAllEngineThreadGauge(GatewayMetrics.ThreadUser.PORT_7443,
                        GatewayMetrics.ThreadStatus.CORE_STA, port7443ThreadCnt);
                GatewayMetrics.setAllEngineThreadGauge(GatewayMetrics.ThreadUser.PORT_7070_Acceptor,
                        GatewayMetrics.ThreadStatus.CORE_STA, port7070ThreadCntAcceptor);
                GatewayMetrics.setAllEngineThreadGauge(GatewayMetrics.ThreadUser.PORT_7443_Acceptor,
                        GatewayMetrics.ThreadStatus.CORE_STA, port7443ThreadCntAcceptor);

                log.info("Start push pushGateWay metric");
                //send metric to pushGateWay
                pushGateway.push(CollectorRegistry.defaultRegistry, config.getKylinMetricsPushGatewayJobName(), groupingKey);
                log.info("Push to pushGateWay Success.");
            } catch (Exception e) {
                log.error("Failed to push metrics to prometheus push gateway Exception is ", e);
                sendFailEmail(e.toString());
            } catch (Error e) {
                log.error("Failed to push metrics to prometheus push gateway Error is ", e);
                sendFailEmail(e.toString());
            }
        }, 5000, config.getKylinMetricsPushGatewayPeriod(), TimeUnit.MILLISECONDS);
    }

    private void sendFailEmail(String e) {
        if (KylinConfig.getInstanceFromEnv().isFailEmailInKylinMetricsPushGatewayEnable()) {
            String[] receiver = config.getAdminDls();
            String tiltle = "Fail to send metric to prometheus pushGateway";
            if (receiver != null) {
                new MailService(config).sendMail(Arrays.asList(receiver), tiltle, e.toString());
            }
        }
    }

    private boolean isQueryThread(String threadName) {
        if (!threadName.startsWith("Query ")) {
            return false;
        }
        try {
            Long.parseLong(threadName.substring(threadName.lastIndexOf('-') + 1)); // thread id
        } catch (Exception e) {
            return false;
        }
        return true;
    }

    private boolean isThreadRunningKylinCode(ThreadInfo threadInfo) {
        StackTraceElement[] stackTraces = threadInfo.getStackTrace();
        for (StackTraceElement stackTrace : stackTraces) {
            if (stackTrace.getClassName().contains("kylin")) {
                return true;
            }
        }
        return false;
    }

}
