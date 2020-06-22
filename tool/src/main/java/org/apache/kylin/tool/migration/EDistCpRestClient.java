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

package org.apache.kylin.tool.migration;

import java.io.IOException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.http.HttpResponse;
import org.apache.http.NoHttpResponseException;
import org.apache.http.client.HttpRequestRetryHandler;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.ssl.SSLSocketFactory;
import org.apache.http.conn.ssl.TrustStrategy;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.PoolingClientConnectionManager;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.params.HttpConnectionParams;
import org.apache.http.params.HttpParams;
import org.apache.http.protocol.HttpContext;
import org.apache.http.util.EntityUtils;
import org.apache.kylin.common.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.sun.tools.javac.util.Assert;

public class EDistCpRestClient {

    private static final Logger logger = LoggerFactory.getLogger(EDistCpRestClient.class);

    protected static final int HTTP_CONNECTION_TIMEOUT_MS = 30000;
    protected static final int HTTP_SOCKET_TIMEOUT_MS = 120000;

    protected static final long ONE_MINUTE = 60000L;

    protected static final int retryCount = 3;

    protected static final ObjectMapper mapper = new ObjectMapper();

    protected DefaultHttpClient client;
    private final String baseUrl;

    private final long distCpJobMaxDuration;
    private final long distCpJobCheckInterval;
    private final ETokenGenerator tokenGenerator;

    private final String doAsName;

    private final int fileAttrKept;

    public EDistCpRestClient(String url, long distCpJobMaxDuration, long distCpJobCheckInterval, String tokenUrl,
            String apiKey, String apiSecret, String doAsName, int fileAttrKept) {
        this(url, distCpJobMaxDuration, distCpJobCheckInterval, tokenUrl, apiKey, apiSecret, doAsName, fileAttrKept, 8);
    }

    public EDistCpRestClient(String url, long distCpJobMaxDuration, long distCpJobCheckInterval, String tokenUrl,
            String apiKey, String apiSecret, String doAsName, int fileAttrKept, int maxPerRoute) {
        this.distCpJobMaxDuration = distCpJobMaxDuration * ONE_MINUTE;
        this.distCpJobCheckInterval = distCpJobCheckInterval * ONE_MINUTE;
        this.baseUrl = url;
        this.tokenGenerator = new ETokenGenerator(tokenUrl, apiKey, apiSecret);
        this.doAsName = doAsName;
        this.fileAttrKept = fileAttrKept;

        final HttpParams httpParams = new BasicHttpParams();
        HttpConnectionParams.setSoTimeout(httpParams, HTTP_SOCKET_TIMEOUT_MS);
        HttpConnectionParams.setConnectionTimeout(httpParams, HTTP_CONNECTION_TIMEOUT_MS);

        PoolingClientConnectionManager pccm = new PoolingClientConnectionManager();
        pccm.setDefaultMaxPerRoute(maxPerRoute);
        pccm.setMaxTotal(maxPerRoute);
        client = new DefaultHttpClient(pccm, httpParams);
        client.setHttpRequestRetryHandler(new HttpRequestRetryHandler() {
            @Override
            public boolean retryRequest(IOException exception, int executionCount, HttpContext context) {
                if (executionCount > retryCount) {
                    logger.warn("Maximum tries reached for client http pool ");
                    return false;
                }
                if (exception instanceof NoHttpResponseException) {
                    logger.warn("No response from server on " + executionCount + " call");
                    return true;
                }
                return false;
            }
        });

        // trust all certificates
        if (baseUrl.startsWith("https")) {
            try {
                SSLSocketFactory sslsf = new SSLSocketFactory(new TrustStrategy() {
                    public boolean isTrusted(final X509Certificate[] chain, String authType)
                            throws CertificateException {
                        // Oh, I am easy...
                        return true;
                    }
                });
                client.getConnectionManager().getSchemeRegistry().register(new Scheme("https", 443, sslsf));
            } catch (Exception e) {
                throw new RuntimeException("Initialize HTTPS client failed", e);
            }
        }
    }

    private void addHttpHeaders(HttpRequestBase method) {
        method.addHeader("Accept", "application/json, text/plain, */*");
        method.addHeader("Content-Type", "application/json");
        method.addHeader("AUTH_TYPE", "KEYSTONE");
        method.addHeader("KEYSTONE_TOKEN", tokenGenerator.generateToken());
    }

    public HttpResponse doGet(String url) throws Exception {
        HttpGet request = new HttpGet(baseUrl + url);
        addHttpHeaders(request);
        return client.execute(request);
    }

    public HttpResponse doPost(String url, String jsonRequest) throws Exception {
        HttpPost request = new HttpPost(baseUrl + url);
        if (!Strings.isNullOrEmpty(jsonRequest)) {
            request.setEntity(new StringEntity(jsonRequest, "UTF-8"));
        }
        addHttpHeaders(request);
        return client.execute(request);
    }

    public String submitDistCpJob(String srcDir, String dstDir, boolean aclsEnabled) throws Exception {
        StringBuilder requestJson = new StringBuilder();
        requestJson.append("{")//
                .append("\"src\": \"").append(srcDir).append("\"")//
                .append(", \"dst\": \"").append(dstDir).append("\"")//
                .append(", \"doAs\": \"").append(doAsName).append("\"");
        if (!aclsEnabled) {
            requestJson.append(", \"fileAttrKept\": ").append(fileAttrKept);
        }
        requestJson.append("}");

        HttpResponse response = doPost("", requestJson.toString());
        String responseContent = EntityUtils.toString(response.getEntity());
        logger.info("Response for submit distcp job from {} to {}: {}", srcDir, dstDir, responseContent);
        JsonNode root = mapper.readTree(responseContent);
        return root.get("result").get("jobId").toString();
    }

    public void waitForJobFinish(String jobId) throws Exception {
        long jobEndTime = System.currentTimeMillis() + distCpJobMaxDuration;
        while (System.currentTimeMillis() < jobEndTime) {
            String jobStatus = "1";
            try {
                jobStatus = checkDistCpJobStatus(jobId);
            } catch (Exception e) {
                logger.warn("Fail to get job status due to ", e);
            }
            switch (jobStatus) {
            case "0":
                jobEndTime = System.currentTimeMillis() + distCpJobMaxDuration;
                break;
            case "2":
                return;
            case "3":
                throw new RuntimeException("Job " + jobId + " fails");
            case "1":
            default:
            }
            //sleep some time and then try to get job status again
            logger.info("Job {} is still running, waiting...", jobId);
            Thread.currentThread().sleep(distCpJobCheckInterval);
        }
        throw new RuntimeException(
                "Job " + jobId + " is not finished in " + distCpJobMaxDuration / ONE_MINUTE + " minutes!");
    }

    public String checkDistCpJobStatus(String jobId) throws Exception {
        HttpResponse response = doGet("/" + jobId);
        String responseContent = EntityUtils.toString(response.getEntity());
        logger.info("Response for check the status of distcp job {}: {}", jobId, responseContent);
        JsonNode root = mapper.readTree(responseContent);
        return root.get("result").get("status").toString();
    }

    private class ETokenGenerator {

        private final String tokenUrl;
        private final String apiKey;
        private final String apiSecret;
        private final FastDateFormat dateFormat;
        private volatile long nextGenTime = 0L;
        private volatile String token;

        public ETokenGenerator(String tokenUrl, String apiKey, String apiSecret) {
            this.tokenUrl = tokenUrl;
            this.apiKey = apiKey;
            this.apiSecret = apiSecret;
            this.dateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss");
        }

        public String generateToken() {
            if (System.currentTimeMillis() > nextGenTime || token == null) {
                synchronized (ETokenGenerator.class) {
                    if (System.currentTimeMillis() > nextGenTime || token == null) {
                        try {
                            Pair<String, Long> tokenAndTime = doGenerateToken();
                            token = tokenAndTime.getFirst();
                            nextGenTime = tokenAndTime.getSecond();
                        } catch (Throwable e) {
                            throw new RuntimeException(e);
                        }
                    }
                }
            }
            return token;
        }

        private long parseTime(String fTimeStr) throws Exception {
            String dateStr = fTimeStr.substring(0, 10);
            String timeStr = fTimeStr.substring(11, 19);
            return dateFormat.parse(dateStr + " " + timeStr).getTime();
        }

        private long updateNextGenTime(String issueTimeStr, String expireTimeStr) throws Exception {
            long issueTime = parseTime(issueTimeStr);
            long expireTime = parseTime(expireTimeStr);
            return System.currentTimeMillis() + (expireTime - issueTime) / 2;
        }

        private Pair<String, Long> doGenerateToken() throws Exception {
            HttpPost request = new HttpPost(tokenUrl);
            request.addHeader("Accept", "application/json, text/plain, */*");
            request.addHeader("Content-Type", "application/json");

            StringBuilder requestJson = new StringBuilder();
            requestJson.append("{")//
                    .append("\"auth\": ")//
                    .append("{")//
                    .append("\"passwordCredentials\": ")//
                    .append("{")//
                    .append("\"username\": \"").append(apiKey).append("\"")//
                    .append(", \"password\": \"").append(apiSecret).append("\"")//
                    .append("}}}");
            request.setEntity(new StringEntity(requestJson.toString(), "UTF-8"));

            HttpResponse response = client.execute(request);
            String responseContent = EntityUtils.toString(response.getEntity());
            logger.info("Get renewed token for {}", apiKey);
            JsonNode root = mapper.readTree(responseContent);
            JsonNode tokenRoot = root.get("access").get("token");
            String issueTimeStr = unwrapQuotation(tokenRoot.get("issued_at").toString());
            String expireTimeStr = unwrapQuotation(tokenRoot.get("expires").toString());
            long nextGenTime = updateNextGenTime(issueTimeStr, expireTimeStr);
            return new Pair<>(unwrapQuotation(tokenRoot.get("id").toString()), nextGenTime);
        }

        private String unwrapQuotation(String qStr) {
            return qStr.substring(1, qStr.length() - 1);
        }
    }

    public static void main(String[] args) throws Exception {
        Assert.check(args.length == 8);
        String url = args[0];
        String tokenUrl = args[1];
        String apiKey = args[2];
        String apiSecret = args[3];
        String srcDir = args[4];
        String dstDir = args[5];
        String doAsName = args[6];
        int fileAttrKept = Integer.parseInt(args[7]);
        EDistCpRestClient client = new EDistCpRestClient(url, 60L, 1L, tokenUrl, apiKey, apiSecret, doAsName,
                fileAttrKept);
        client.tokenGenerator.generateToken();
        String jobId = client.submitDistCpJob(srcDir, dstDir, false);
        client.checkDistCpJobStatus(jobId);
    }
}
