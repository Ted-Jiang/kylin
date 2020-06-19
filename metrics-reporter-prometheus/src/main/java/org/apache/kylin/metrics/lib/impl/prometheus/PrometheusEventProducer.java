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

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exceptions.ResourceLimitExceededException;
import org.apache.kylin.metrics.lib.Record;
import org.apache.kylin.metrics.property.JobPropertyEnum;
import org.apache.kylin.metrics.property.QueryPropertyEnum;
import org.apache.kylin.metrics.property.QueryRPCPropertyEnum;
import org.apache.kylin.stream.core.util.NamedThreadFactory;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import io.prometheus.client.exporter.MetricsServlet;

public class PrometheusEventProducer {
    private static final Logger logger = LoggerFactory.getLogger(PrometheusEventProducer.class);
    private Server server;

    private Gauge queryLatencyGauge;
    private Gauge queryRPCLatencyGauge; //Only for RPC with exceptions
    private Counter queryCounter;
    private Gauge queryAvailabilityGauge;
    private Gauge slowQueryPercentage;
    private Counter badQueryCounter;
    private Counter slowQueryCounter;
    private Counter normalQueryCounter;
    private Counter queryHitCacheCounter;
    private Gauge queryHitCachePercentage;
    private Counter scanOutOfLimitExCounter;

    //end point rpc exception counter
    private Counter epRPCExceptionCounter;

    private Counter errorJobCounter;
    private Counter jobCounter;
    private Gauge errorJobPercentage;
    private Gauge jobDurationGauge;
    private Gauge buildDictDurationGauge;
    private Gauge distinctColDurationGauge;
    private Gauge convertHFileDurationGauge;
    private Gauge inmemCubingDurationGauge;
    private Gauge waitResDurationGauge;

    private Gauge totalThreadsGauge;
    private Gauge usedTomcatThreadsGauge;
    private Gauge usedQueryThreadsGauge;
    private Gauge usedCoprocessorThreadsGauge;
    private Gauge hbaseIPCThreadsGauge;
    private Gauge sharedHConnectionThreadsGauge;

    private ScheduledExecutorService appMetricsCollectorExecutor;

    public PrometheusEventProducer(Properties props) throws Exception {
        String host = props.getProperty("agent.host");
        if (host == null || host == "") {
            logger.warn("agent host not configured");
            host = InetAddress.getLocalHost().getHostName();
        }
        String port = props.getProperty("agent.port", "1997");
        start(host, Integer.valueOf(port));
        initMetrics();
        appMetricsCollectorExecutor = Executors
                .newSingleThreadScheduledExecutor(new NamedThreadFactory("metrics-collector"));
        appMetricsCollectorExecutor.scheduleAtFixedRate(new AppMetricsCollector(), 60, 60, TimeUnit.SECONDS);
    }

    private void initMetrics() {
        queryLatencyGauge = Gauge.build().name("query_latency").help("query latency")
                .labelNames("project_name", "cube_name").register();

        queryRPCLatencyGauge = Gauge.build().name("query_rpc_latency").help("query rpc latency")
                .labelNames("project_name", "cube_name").register();

        queryCounter = Counter.build().name("query_count").help("query count").labelNames("project_name", "cube_name")
                .register();

        scanOutOfLimitExCounter = Counter.build().name("scanoutoflimit_exception_count")
                .help("scan out of limit exception count").labelNames("project_name", "cube_name").register();

        queryAvailabilityGauge = Gauge.build().name("query_availability").help(
                "query availability: (count of queries with HBase rpc exception, exclude ScanOutOfLimitException)/(query count)")
                .labelNames("project_name", "cube_name").register();

        badQueryCounter = Counter.build().name("bad_query_count").help("query with any exception count")
                .labelNames("project_name", "cube_name").register();

        slowQueryCounter = Counter.build().name("slow_query").help("query with long latency count")
                .labelNames("project_name", "cube_name").register();

        slowQueryPercentage = Gauge.build().name("slow_query_percentage")
                .help("slow query percentage: (slow query count)/(slow query and normal query count)")
                .labelNames("project_name", "cube_name").register();

        normalQueryCounter = Counter.build().name("normal_query_count").help("query with normal latency count")
                .labelNames("project_name", "cube_name").register();

        queryHitCacheCounter = Counter.build().name("query_hit_cache_count").help("query hit cache count")
                .labelNames("project_name", "cube_name").register();

        queryHitCachePercentage = Gauge.build().name("query_hit_cache_percentage").help("query hit cache percentage")
                .labelNames("project_name", "cube_name").register();

        epRPCExceptionCounter = Counter.build().name("endpoint_rpc_exception_count")
                .help("query with HBase coprocessor endpoint exception count, exclude ScanOutOfLimitException")
                .labelNames("project_name", "cube_name").register();

        errorJobCounter = Counter.build().name("error_job_count").help("error job count")
                .labelNames("project_name", "cube_name", "job_type").register();

        jobCounter = Counter.build().name("job_count").help("job count")
                .labelNames("project_name", "cube_name", "job_type").register();

        errorJobPercentage = Gauge.build().name("error_job_percentage").help("error job percentage")
                .labelNames("project_name", "cube_name", "job_type").register();

        jobDurationGauge = Gauge.build().name("cube_build_duration").help("build cube job duration")
                .labelNames("project_name", "cube_name", "job_type").register();

        buildDictDurationGauge = Gauge.build().name("build_dictionary_duration").help("build dictionary step duration")
                .labelNames("project_name", "cube_name").register();

        distinctColDurationGauge = Gauge.build().name("distinct_columns_duration")
                .help("get distinct columns step duration").labelNames("project_name", "cube_name").register();

        convertHFileDurationGauge = Gauge.build().name("convert_hfile_duration").help("convert HFile step duration")
                .labelNames("project_name", "cube_name").register();

        inmemCubingDurationGauge = Gauge.build().name("inmemory_cubing_duration").help("in-memory cubing step duration")
                .labelNames("project_name", "cube_name").register();

        waitResDurationGauge = Gauge.build().name("wait_resource_duration").help("job wait resource duration")
                .labelNames("project_name", "cube_name", "job_type").register();

        totalThreadsGauge = Gauge.build().name("total_thread_cnt").help("total threads count used by Kylin").register();

        usedTomcatThreadsGauge = Gauge.build().name("used_tomcat_thread_cnt")
                .help("currently used tomcat processing threads count").register();

        usedQueryThreadsGauge = Gauge.build().name("used_query_thread_cnt").help("currently used query threads count")
                .register();

        usedCoprocessorThreadsGauge = Gauge.build().name("used_coprocessor_thread_cnt")
                .help("currently used coprocessor threads count").register();

        hbaseIPCThreadsGauge = Gauge.build().name("ipc_client_thread_cnt").help("current ipc threads count").register();

        sharedHConnectionThreadsGauge = Gauge.build().name("shared_hconn_thread_cnt")
                .help("current shared hconnection threads count").register();
    }

    private void start(String host, int port) throws Exception {
        InetSocketAddress socket = new InetSocketAddress(host, port);
        logger.debug("prometheus agent: " + host + ":" + port);
        server = new Server(socket);
        ServletContextHandler context = new ServletContextHandler();
        context.setContextPath("/");
        server.setHandler(context);
        context.addServlet(new ServletHolder(new MetricsServlet()), "/metrics");
        server.start();
        logger.info("prometheus agent started!");
    }

    public void add(Collection<Record> records) {
        for (Record record : records) {
            add(record);
        }
    }

    public void add(Record record) {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();

        String type = record.getSubject();
        if (type.equalsIgnoreCase(kylinConfig.getKylinMetricsSubjectQueryRpcCall())) {
            addQueryRPCMetrics(record);
        } else if (type.equalsIgnoreCase(kylinConfig.getKylinMetricsSubjectQuery())) {
            addQueryMetrics(record);
        } else if (type.equalsIgnoreCase(kylinConfig.getKylinMetricsSubjectJobException())) {
            addJobExceptionMetrics(record);
        } else if (type.equalsIgnoreCase(kylinConfig.getKylinMetricsSubjectJob())) {
            addJobMetrics(record);
        } else if (type.equalsIgnoreCase(kylinConfig.getKylinMetricsSubjectQueryCube())) {
            //metric METRICS_QUERY already cover metric of query on cube, do nothing here
        } else {
            logger.warn("unknown metrics record type.");
        }
    }

    private void addQueryRPCMetrics(Record record) {
        Map<String, Object> kvs = record.getValueRaw();
        String prjName = String.valueOf(kvs.get(QueryRPCPropertyEnum.PROJECT.toString()));
        String cubeName = String.valueOf(kvs.get(QueryRPCPropertyEnum.REALIZATION.toString()));

        long callTime = Long.valueOf(String.valueOf(kvs.get(QueryRPCPropertyEnum.CALL_TIME.toString())));
        Object exception = kvs.get(QueryRPCPropertyEnum.EXCEPTION.toString());
        if (exception.toString() != "NULL") {
            queryRPCLatencyGauge.labels(prjName, cubeName).set(callTime);
            if (!exception.toString().equalsIgnoreCase(ResourceLimitExceededException.class.getName())) {
                logger.debug("endpoint rpc exception: " + exception.toString());
                epRPCExceptionCounter.labels(prjName, cubeName).inc();
            }
        }
    }

    private void addQueryMetrics(Record record) {
        Map<String, Object> kvs = record.getValueRaw();
        String prjName = String.valueOf(kvs.get(QueryPropertyEnum.PROJECT.toString()));
        String cubeName = String.valueOf(kvs.get(QueryPropertyEnum.REALIZATION.toString()));
        String type = String.valueOf(kvs.get(QueryPropertyEnum.TYPE.toString()));

        long queryLatency = Long.valueOf(String.valueOf(kvs.get(QueryPropertyEnum.TIME_COST.toString())));
        queryLatencyGauge.labels(prjName, cubeName).set(queryLatency);

        Object exception = kvs.get(QueryPropertyEnum.EXCEPTION.toString());
        //both InternalErrorException and other exceptions mark as exception
        if (exception.toString() != "NULL") {
            badQueryCounter.labels(prjName, cubeName).inc();
            if (exception.toString().equalsIgnoreCase(ResourceLimitExceededException.class.getName())) {
                scanOutOfLimitExCounter.labels(prjName, cubeName).inc();
            }
        } else {
            if (queryLatency > Long.valueOf(KylinConfig.getInstanceFromEnv().getBadQueryDefaultAlertingSeconds())) {
                slowQueryCounter.labels(prjName, cubeName).inc();
            } else {
                normalQueryCounter.labels(prjName, cubeName).inc();
            }
        }

        queryCounter.labels(prjName, cubeName).inc();

        double epRPCExceptionCount = epRPCExceptionCounter.labels(prjName, cubeName).get();
        double queryCount = queryCounter.labels(prjName, cubeName).get();
        queryAvailabilityGauge.labels(prjName, cubeName).set(1 - epRPCExceptionCount / queryCount);

        double slowQueryCount = slowQueryCounter.labels(prjName, cubeName).get();
        double normalQueryCount = normalQueryCounter.labels(prjName, cubeName).get();
        slowQueryPercentage.labels(prjName, cubeName).set(slowQueryCount / (slowQueryCount + normalQueryCount));

        if (type != null && type.equals("CACHE")) {
            queryHitCacheCounter.labels(prjName, cubeName).inc();
            double queryHitCacheCount = queryHitCacheCounter.labels(prjName, cubeName).get();
            double queryCount2 = queryCounter.labels(prjName, cubeName).get();
            queryHitCachePercentage.labels(prjName, cubeName).set(queryHitCacheCount / queryCount2);
        }
    }

    //for success job, add duration metrics
    private void addJobMetrics(Record record) {
        Map<String, Object> kvs = record.getValueRaw();
        String prjName = String.valueOf(kvs.get(JobPropertyEnum.PROJECT.toString()));
        String cubeName = String.valueOf(kvs.get(JobPropertyEnum.CUBE.toString()));
        String jobType = String.valueOf(kvs.get(JobPropertyEnum.TYPE.toString()));

        long jobDuration = Long.valueOf(String.valueOf(kvs.get(JobPropertyEnum.BUILD_DURATION.toString())));
        long waitResourceDuration = Long
                .valueOf(String.valueOf(kvs.get(JobPropertyEnum.WAIT_RESOURCE_TIME.toString())));
        long buildDictDuration = Long
                .valueOf(String.valueOf(kvs.get(JobPropertyEnum.STEP_DURATION_DICTIONARY.toString())));
        long distinctColDuration = Long
                .valueOf(String.valueOf(kvs.get(JobPropertyEnum.STEP_DURATION_DISTINCT_COLUMNS.toString())));
        long convertHFileDuration = Long
                .valueOf(String.valueOf(kvs.get(JobPropertyEnum.STEP_DURATION_HFILE_CONVERT.toString())));
        long inmemCubingDuration = Long
                .valueOf(String.valueOf(kvs.get(JobPropertyEnum.STEP_DURATION_INMEM_CUBING.toString())));

        jobDurationGauge.labels(prjName, cubeName, jobType).set(jobDuration);
        buildDictDurationGauge.labels(prjName, cubeName).set(buildDictDuration);
        distinctColDurationGauge.labels(prjName, cubeName).set(distinctColDuration);
        convertHFileDurationGauge.labels(prjName, cubeName).set(convertHFileDuration);
        inmemCubingDurationGauge.labels(prjName, cubeName).set(inmemCubingDuration);

        waitResDurationGauge.labels(prjName, cubeName, jobType).set(waitResourceDuration);
        jobCounter.labels(prjName, cubeName, jobType).inc();

        double errorJobCount = errorJobCounter.labels(prjName, cubeName, jobType).get();
        double jobCount = jobCounter.labels(prjName, cubeName, jobType).get();
        errorJobPercentage.labels(prjName, cubeName, jobType).set(errorJobCount / jobCount);
    }

    private void addJobExceptionMetrics(Record record) {
        Map<String, Object> kvs = record.getValueRaw();
        String prjName = String.valueOf(kvs.get(JobPropertyEnum.PROJECT.toString()));
        String cubeName = String.valueOf(kvs.get(JobPropertyEnum.CUBE.toString()));
        String jobType = String.valueOf(kvs.get(JobPropertyEnum.TYPE.toString()));

        Object exception = kvs.get(JobPropertyEnum.EXCEPTION.toString());
        if (exception != null) {
            errorJobCounter.labels(prjName, cubeName, jobType).inc();
        }
        jobCounter.labels(prjName, cubeName, jobType).inc();
    }

    public void close() throws Exception {
        server.stop();
        logger.info("Prometheus agent closed!");
    }

    public long getQueryLatencyGauge(String prjName, String cubeName) {
        return (long) queryLatencyGauge.labels(prjName, cubeName).get();
    }

    public long getQueryRPCLatencyGauge(String prjName, String cubeName) {
        return (long) queryRPCLatencyGauge.labels(prjName, cubeName).get();
    }

    public long getQueryCounter(String prjName, String cubeName) {
        return (long) queryCounter.labels(prjName, cubeName).get();
    }

    public long getQueryAvailabilityGauge(String prjName, String cubeName) {
        return (long) queryAvailabilityGauge.labels(prjName, cubeName).get();
    }

    public long getSlowQueryPercentage(String prjName, String cubeName) {
        return (long) slowQueryPercentage.labels(prjName, cubeName).get();
    }

    public long getBadQueryCounter(String prjName, String cubeName) {
        return (long) badQueryCounter.labels(prjName, cubeName).get();
    }

    public long getSlowQueryCounter(String prjName, String cubeName) {
        return (long) slowQueryCounter.labels(prjName, cubeName).get();
    }

    public long getNormalQueryCounter(String prjName, String cubeName) {
        return (long) normalQueryCounter.labels(prjName, cubeName).get();
    }

    public long getQueryHitCacheCounter(String prjName, String cubeName) {
        return (long) queryHitCacheCounter.labels(prjName, cubeName).get();
    }

    public long getQueryHitCachePercentage(String prjName, String cubeName) {
        return (long) queryHitCachePercentage.labels(prjName, cubeName).get();
    }

    public long getScanOutOfLimitExCounter(String prjName, String cubeName) {
        return (long) scanOutOfLimitExCounter.labels(prjName, cubeName).get();
    }

    public long getEpRPCExceptionCounter(String prjName, String cubeName) {
        return (long) epRPCExceptionCounter.labels(prjName, cubeName).get();
    }

    public long getJobCounter(String prjName, String cubeName, String jobType) {
        return (long) jobCounter.labels(prjName, cubeName, jobType).get();
    }

    public long getErrorJobPercentage(String prjName, String cubeName, String jobType) {
        return (long) errorJobPercentage.labels(prjName, cubeName, jobType).get();
    }

    public long getErrorJobCounter(String prjName, String cubeName, String jobType) {
        return (long) errorJobCounter.labels(prjName, cubeName, jobType).get();
    }

    public long getJobDurationGauge(String prjName, String cubeName, String jobType) {
        return (long) jobDurationGauge.labels(prjName, cubeName, jobType).get();
    }

    public long getBuildDictDurationGauge(String prjName, String cubeName) {
        return (long) buildDictDurationGauge.labels(prjName, cubeName).get();
    }

    public long getDistinctColDurationGauge(String prjName, String cubeName) {
        return (long) distinctColDurationGauge.labels(prjName, cubeName).get();
    }

    public long getConvertHFileDurationGauge(String prjName, String cubeName) {
        return (long) convertHFileDurationGauge.labels(prjName, cubeName).get();
    }

    public long getInmemCubingDurationGauge(String prjName, String cubeName) {
        return (long) inmemCubingDurationGauge.labels(prjName, cubeName).get();
    }

    public long getWaitResDurationGauge(String prjName, String cubeName, String jobType) {
        return (long) waitResDurationGauge.labels(prjName, cubeName, jobType).get();
    }

    private class AppMetricsCollector implements Runnable {

        @Override
        public void run() {
            try {
                // TODO collect tomcat information from tomcat JMX?
                ThreadInfo[] allThreads = ManagementFactory.getThreadMXBean().dumpAllThreads(false, false);
                int usedTomcatThreadCnt = 0;
                int usedQueryThreadCnt = 0;
                int usedCoprocessorThreadCnt = 0;
                int totalThreadCnt = 0;
                int ipcThreadCnt = 0;
                int sharedHConnThreadCnt = 0;
                for (ThreadInfo threadInfo : allThreads) {
                    totalThreadCnt++;
                    String threadName = threadInfo.getThreadName();
                    if (threadName == null) {
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
                    }
                }
                usedQueryThreadsGauge.set(usedQueryThreadCnt);
                usedTomcatThreadsGauge.set(usedTomcatThreadCnt);
                usedCoprocessorThreadsGauge.set(usedCoprocessorThreadCnt);
                hbaseIPCThreadsGauge.set(ipcThreadCnt);
                sharedHConnectionThreadsGauge.set(sharedHConnThreadCnt);
                totalThreadsGauge.set(totalThreadCnt);
            } catch (Exception e) {
                logger.error("error when collect app metrics", e);
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
}
