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

package org.apache.kylin.stream.source.rheos;

import io.ebay.rheos.http.RheosHTTPClientConfig;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.shaded.com.google.common.collect.Maps;

import java.util.Map;

import static org.apache.kylin.common.KylinConfigBase.SOURCE_RHEOS_CONFIG_PREFIX;

public class RheosConfig {
    private final KylinConfig kylinConfig;
    private final Map<String, String> rheosConfigMap;

    public RheosConfig(KylinConfig kylinConfig) {
        this.kylinConfig = kylinConfig;
        this.rheosConfigMap = kylinConfig.getRheosConfigs();
    }

    private String getRequired(String propKey) {
        String r = rheosConfigMap.get(propKey);
        if (StringUtils.isEmpty(r)) {
            throw new IllegalArgumentException("missing '" + SOURCE_RHEOS_CONFIG_PREFIX + propKey + "' in conf/kylin.properties");
        }
        return r;
    }

    private String getOptional(String propKey) {
        return getOptional(propKey, null);
    }

    private String getOptional(String propKey, String dft) {
        String r = rheosConfigMap.get(propKey);
        if (StringUtils.isEmpty(r)) {
            return dft;
        }
        return r;
    }

    public String getSupportKeyStoneApiKey() {
        return kylinConfig.getEBaySupportKeyStoneApiKey();
    }

    public String getSupportKeyStoneApiSecret() {
        return kylinConfig.getEBaySupportKeyStoneApiSecret();
    }

    public String getIAFConsumerId() {
        return getRequired("iaf-consumer-id");
    }

    public String getIAFConsumerSecret() {
        return getRequired("iaf-consumer-secret");
    }

    public String getRheosEnv() {
        return getRequired("environment");
    }

    public String getRheosManagementURL() {
        String baseApiPath = getOptional("base-api-path", "/api/v1");
        return getRheosServiceURL() + baseApiPath;
    }

    public String getRheosServiceURL() {
        return getRequired("service-url");
    }

    public boolean getRhoesHttpClientSSLCheckEnabled() {
        return Boolean.parseBoolean(getOptional("ssl-check-enabled", "true"));
    }

    // in millisecond
    public int getRheosHttpClientConnectTimeout() {
        return Integer.parseInt(getOptional("connect-timeout", "10000"));
    }

    // in millisecond
    public int getRheosHttpClientReadTimeout() {
        return Integer.parseInt(getOptional("read-timeout", "10000"));
    }

    public String getRheosKeystorePath() {
        return getRequired("keystore-path");
    }

    public String getRheosKeystorePassword() {
        return getRequired("keystore-password");
    }

    public String getRheosTruststorePath() {
        return getRequired("truststore-path");
    }

    public String getRheosTruststorePassword() {
        return getRequired("truststore-password");
    }

    public String getRheosConsumerName() {
        return getOptional("consumer-name");
    }

    public String getRheosDC() {
        return getOptional("dc");
    }

    public String getRheosBootstrapServers() {
        return getOptional("bootstrap-servers");
    }

    public Map<String, String> getSSLProps() {
        Map<String, String> config = Maps.newHashMap();
        if (getRhoesHttpClientSSLCheckEnabled()) {
            config.put(RheosHTTPClientConfig.HTTPS_KEYSTORE_PATH, getRheosKeystorePath());
            config.put(RheosHTTPClientConfig.HTTPS_KEYSTORE_PASSWORD, getRheosKeystorePassword());
            config.put(RheosHTTPClientConfig.HTTPS_TRUSTSTORE_PATH, getRheosTruststorePath());
            config.put(RheosHTTPClientConfig.HTTPS_TRUSTSTORE_PASSWORD, getRheosTruststorePassword());
        } else {
            config.put(RheosHTTPClientConfig.HTTPS_KEYSTORE_PATH, RheosHTTPClientConfig.DEFAULT_NO_VALUE);
        }
        return config;
    }
}
