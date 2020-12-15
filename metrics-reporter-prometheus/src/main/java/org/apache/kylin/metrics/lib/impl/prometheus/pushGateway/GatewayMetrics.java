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

import io.prometheus.client.Gauge;
import io.prometheus.client.hotspot.DefaultExports;
import org.apache.kylin.common.util.ToolUtil;

public class GatewayMetrics {

    private static Gauge queryEngineThreadGauge;
    private static Gauge jobEngineThreadGauge;
    private static Gauge allEngineThreadGauge;
    private static Gauge cacheGauge;

    static class ThreadUser {
        public static String TOMCAT = "Tomcat";
        public static String HBASE_PRC = "HBase_Coprocessor_RPC";
        public static String HBASE_IPC = "HBase_IPC";
        public static String MEMCACHED = "Memcached";
        public static String LOAD_LOOKUP_SNAPSHOT = "Loading_Lookup_Table_Snapshot";
        public static String JOB_SCHEDULER = "Job_Scheduler";
        public static String HBASE_OPERATION = "HBase_Operation_Meta";
        public static String PORT_7443 = "Port_7443";
        public static String PORT_7443_Acceptor = "Port_7443_Acceptor";
        public static String PORT_7070 = "Port_7070";
        public static String PORT_7070_Acceptor = "Port_7070_Acceptor";
    }

    static class CacheCategory {
        public static String DICT_CACHE = "Dictionary_Cache";
        public static String LOOKUP_CACHE = "Lookup_Table_Dictionary_Cache";
    }

    static class CacheStatus {
        public static String SIZE = "Size";
        public static String NUM = "Number";
        public static String EVICT = "Evict_Number";
    }

    static class ThreadStatus {
        public static String CORE_STA = "core";
        public static String QUEUE_STA = "queue";
    }

    public static void initialize() {
        DefaultExports.initialize();
        queryEngineThreadGauge = Gauge.build().name("queryEngine_thread_cnt").help("queryEngine thread count")
                .labelNames("host", "user", "status").register();
        jobEngineThreadGauge = Gauge.build().name("jobEngine_thread_cnt").help("jobEngine thread count")
                .labelNames("host", "user", "status").register();
        allEngineThreadGauge = Gauge.build().name("allEngine_thread_cnt").help("allEngine thread count")
                .labelNames("host", "user", "status").register();
        cacheGauge = Gauge.build().name("cache_cnt").help("cache count").labelNames("host", "category", "sta")
                .register();
    }

    private static final String host = ToolUtil.getHostName();

    public static void incQueryEngineThreadGauge(String user, String sta) {
        queryEngineThreadGauge.labels(host, user, sta).inc();
    }

    public static void decQueryEngineThreadGauge(String user, String sta) {
        queryEngineThreadGauge.labels(host, user, sta).dec();
    }

    public static void setQueryEngineThreadGauge(String user, String sta, int cnt) {
        queryEngineThreadGauge.labels(host, user, sta).set(cnt);
    }

    public static void incJobEngineThreadGauge(String user, String sta) {
        jobEngineThreadGauge.labels(host, user, sta).inc();
    }

    public static void decJobEngineThreadGauge(String user, String sta) {
        jobEngineThreadGauge.labels(host, user, sta).dec();
    }

    public static void setJobEngineThreadGauge(String user, String sta, int cnt) {
        jobEngineThreadGauge.labels(host, user, sta).set(cnt);
    }

    public static void incAllEngineThreadGauge(String user, String sta) {
        allEngineThreadGauge.labels(host, user, sta).inc();
    }

    public static void decAllEngineThreadGauge(String user, String sta) {
        allEngineThreadGauge.labels(host, user, sta).dec();
    }

    public static void setAllEngineThreadGauge(String user, String sta, int cnt) {
        allEngineThreadGauge.labels(host, user, sta).set(cnt);
    }

    public static void setCacheGauge(String category, String sta, long cnt) {
        cacheGauge.labels(host, category, sta).set(cnt);
    }

}
