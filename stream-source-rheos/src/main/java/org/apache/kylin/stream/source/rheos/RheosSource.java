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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.StringUtil;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.shaded.com.google.common.annotations.VisibleForTesting;
import org.apache.kylin.shaded.com.google.common.base.Preconditions;
import org.apache.kylin.stream.core.consumer.ConsumerStartProtocol;
import org.apache.kylin.stream.core.consumer.IStreamingConnector;
import org.apache.kylin.stream.core.exception.StreamingException;
import org.apache.kylin.stream.core.source.Partition;
import org.apache.kylin.stream.core.source.StreamingSourceConfig;
import org.apache.kylin.stream.core.source.StreamingSourceConfigManager;
import org.apache.kylin.stream.core.storage.StreamingSegmentManager;
import org.apache.kylin.stream.source.kafka.KafkaSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;

import io.ebay.rheos.http.RheosHttpClient;
import io.ebay.rheos.http.StringResponseHandler;
import io.ebay.rheos.kafka.security.iaf.IAFLoginModule;
import joptsimple.internal.Strings;

public class RheosSource extends KafkaSource {
    private static final Logger logger = LoggerFactory.getLogger(RheosSource.class);

    public static final String PROP_TOPIC_NAMESPACE = "topicNamespace";
    public static final String PROP_TOPIC_STREAM = "topicStream";
    public static final String PROP_TOPIC_CONSUMER_NAME = "topicConsumerName";
    public static final String PROP_TOPIC_DC = "topicDC";
    public static final String PROP_TOPIC_EVENT_TYPE = "topicEventType";
    private static final String RHEOS_EVENT_MESSAGE_PARSE_CLASS = RheosStreamParser.class.getName();

    private final RheosConfig rheosConfig;

    public RheosSource() {
        this.rheosConfig = new RheosConfig(KylinConfig.getInstanceFromEnv());
    }

    public RheosConfig getRheosConfig() {
        return rheosConfig;
    }

    @Override
    public String getMessageTemplate(StreamingSourceConfig streamingSourceConfig) throws StreamingException {
        try {
            String rheosEventSchema = getRheosTopicSchemaByRest(streamingSourceConfig);
            Schema avroSchema = new Schema.Parser().parse(rheosEventSchema);
            return MessageTemplateUtils.convertAvroSchemaToJson(avroSchema);
        } catch (IOException e) {
            throw new StreamingException(e);
        }
    }

    @Override
    public IStreamingConnector createStreamingConnector(String cubeName, List<Partition> assignedPartitions,
            ConsumerStartProtocol startProtocol, StreamingSegmentManager streamingSegmentManager) {
        // update the property of PROP_MESSAGE_PARSER according to the PROP_TOPIC_EVENT_TYPE
        logger.info("Create RheosStreamingConnector for Cube {}, assignedPartitions {}, startProtocol {}", cubeName,
                assignedPartitions, startProtocol);
        KylinConfig kylinConf = KylinConfig.getInstanceFromEnv();
        CubeInstance cubeInstance = CubeManager.getInstance(kylinConf).getCube(cubeName);
        String streamingName = cubeInstance.getRootFactTable();
        StreamingSourceConfig streamingSourceConfig = StreamingSourceConfigManager.getInstance(kylinConf)
                .getConfig(streamingName);
        Map<String, String> properties = streamingSourceConfig.getProperties();
        if (properties.get(PROP_TOPIC_EVENT_TYPE) == null) {
            throw new StreamingException(
                    "event type missing for creating rheos streaming connector, properties of source:" + properties);
        } else {
            EventType eventType;
            eventType = EventType.valueOf(properties.get(PROP_TOPIC_EVENT_TYPE));
            switch (eventType) {
            case RHEOS_EVENT:
                properties.put(PROP_MESSAGE_PARSER, RHEOS_EVENT_MESSAGE_PARSE_CLASS);
                break;
            // TODO: add other event type
            default:
                throw new StreamingException("unknown event type: " + eventType);
            }
        }
        // update stream source config
        try {
            StreamingSourceConfigManager.getInstance(kylinConf).updateStreamingConfig(streamingSourceConfig);
        } catch (IOException e) {
            throw new StreamingException(e);
        }
        return super.createStreamingConnector(cubeName, assignedPartitions, startProtocol, streamingSegmentManager);
    }

    @Override
    protected Map<String, Object> getKafkaConf(Map<String, String> sourceProperties, KylinConfig kylinConfig) {
        RheosConfig realRheosConfig = new RheosConfig(kylinConfig);
        String consumerName = mustGetRheosTopicConsumerName(realRheosConfig, sourceProperties);
        String bootstrapServers = mustGetRheosTopicBootstrapServers(realRheosConfig, sourceProperties);

        // To avoid side effect
        Map<String, String> copiedSourceProperties = Maps.newHashMap(sourceProperties);
        copiedSourceProperties.put(PROP_BOOTSTRAP_SERVERS, bootstrapServers);
        copiedSourceProperties.put(ConsumerConfig.GROUP_ID_CONFIG, consumerName);

        Map<String, Object> kafkaConf = getKafkaConf(copiedSourceProperties);
        {// Override configuration
            Map<String, String> kafkaConfigOverride = kylinConfig.getKafkaConfigOverride();
            kafkaConf.putAll(kafkaConfigOverride);
        }
        return kafkaConf;
    }

    @Override
    protected Map<String, Object> getKafkaConf(Map<String, String> sourceProperties) {
        Map<String, Object> conf = Maps.newHashMap();
        {// Common kafka configuration
            conf.putAll(super.getKafkaConf(sourceProperties));
        }
        {// Rheos specified configuration
            String saslJaasConfig = getSaslJaasConfig();
            String securityMode = getRheosSecurityModeByRest(sourceProperties);

            conf.put(ConsumerConfig.GROUP_ID_CONFIG, sourceProperties.get(ConsumerConfig.GROUP_ID_CONFIG));
            conf.put(SaslConfigs.SASL_MECHANISM, "IAF");
            conf.put(SaslConfigs.SASL_JAAS_CONFIG, saslJaasConfig);
            conf.put("security.protocol", securityMode);
            if (isSSLEnabled(securityMode)) {
                conf.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "");
            }
        }
        return conf;
    }

    public String getRheosTopicSchemaByRest(StreamingSourceConfig streamingSourceConfig) throws IOException {
        Map<String, String> sourceProperties = streamingSourceConfig.getProperties();
        String authValue = getKeyStoneToken(sourceProperties);
        String topicId = getRheosTopicId(sourceProperties);
        String url = String.format(Locale.ROOT, "%s/schemas/topics/%s/subjects", rheosConfig.getRheosManagementURL(), topicId);
        String topicSubjectInfo = runGet(url, authValue);
        String schemaName = parseAndGetRheosTopicSubject(topicSubjectInfo);
        if (StringUtil.isEmpty(schemaName)) {
            return super.getMessageTemplate(streamingSourceConfig);
        }
        url = String.format(Locale.ROOT, "%s/schemas/subjects/%s/versions/latest", rheosConfig.getRheosManagementURL(), schemaName);
        String schemaDetails = runGet(url, authValue);
        return parseAndGetRheosTopicSchemaContent(schemaDetails);
    }

    public String getRheosTopicId(Map<String, String> sourceProperties) {
        String topicId = getRheosTopicIdByRest(sourceProperties);
        return topicId;
    }

    private String getRheosTopicIdByRest(Map<String, String> sourceProperties) {
        String streamName = mustGetTopicStream(sourceProperties);
        // must get the topic name
        String topicName = getTopicName(sourceProperties);
        Preconditions.checkNotNull(topicName, "topic name should not be null");
        Preconditions.checkArgument(topicName.length() != 0, "topic name should not be empty");

        String url = String.format(Locale.ROOT, "%s/streams/%s/topics/%s", rheosConfig.getRheosManagementURL(), streamName, topicName);
        try {
            String authValue = getKeyStoneToken(sourceProperties);
            String topicDetails = runGet(url, authValue);
            return parseAndGetRheosTopicId(topicDetails);
        } catch (IOException e) {
            throw new StreamingException(e.getCause());
        }
    }

    private String getRheosSecurityModeByRest(Map<String, String> sourceProperties) {
        String streamName = mustGetTopicStream(sourceProperties);
        String url = String.format(Locale.ROOT, "%s/streams/%s", rheosConfig.getRheosManagementURL(), streamName);
        try {
            String authValue = getKeyStoneToken(sourceProperties);
            String streamDetails = runGet(url, authValue);
            return parseAndGetRheosSecurityMode(streamDetails);
        } catch (IOException e) {
            throw new StreamingException(e.getCause());
        }
    }

    /**
     * First check in realRheosConfig,
     * if not exist, check by rest call
     * if not exist, check in sourceProperties
     */
    public String mustGetRheosTopicBootstrapServers(RheosConfig realRheosConfig, Map<String, String> sourceProperties) {
        // User can manually specify the bootstrap servers for each cube
        String bootstrapServers = realRheosConfig.getRheosBootstrapServers();
        if (!Strings.isNullOrEmpty(bootstrapServers)) {
            logger.info("Get by cube level config of {}", PROP_BOOTSTRAP_SERVERS);
            return bootstrapServers;
        }
        // User can manually set the bootstrap servers.
        bootstrapServers = getBootstrapServers(sourceProperties);
        if (!Strings.isNullOrEmpty(bootstrapServers)) {
            logger.info("Get by table level config of {}", PROP_BOOTSTRAP_SERVERS);
            return bootstrapServers;
        }
        // User can manually specify the consumer name + dc name for each cube to get bootstrap servers by restful API
        String consumerName = mustGetRheosTopicConsumerName(realRheosConfig, sourceProperties);
        String dcName = mustGetRheosTopicDC(realRheosConfig, sourceProperties);
        if (!StringUtil.isEmpty(consumerName) && !StringUtil.isEmpty(dcName)) {
            logger.info("Get by cube level config of consumer name {} and dc name {}", consumerName, dcName);
            bootstrapServers = getRheosTopicBootstrapServersByRest(consumerName, dcName);
            return bootstrapServers;
        }
        throw new IllegalArgumentException("Fail to get bootstrap servers. Please set property of (" + PROP_BOOTSTRAP_SERVERS + ") or (consumer name, dc name)");
    }

    public String getRheosTopicBootstrapServersByRest(String consumerName, String dcName) {
        throw new StreamingException("Not support getting bootstrap servers from consumer and dcName");
    }


    private String getSaslJaasConfig() {
        return String.format(Locale.ROOT, "%s required iafConsumerId=\"%s\" iafSecret=\"%s\" iafEnv=\"%s\";", IAFLoginModule.class.getName(), rheosConfig.getIAFConsumerId(), rheosConfig.getIAFConsumerSecret(), rheosConfig.getRheosEnv());
    }

    private String runGet(String url, String authValue) throws IOException {
        RheosHttpClient httpClient = getRheosHttpClient();
        Map<String, String> headers = Maps.newHashMap();
        headers.put("Content-Type", "application/json");
        headers.put("Authorization", authValue);

        return httpClient.get(url, headers, new StringResponseHandler());
    }

    private RheosHttpClient getRheosHttpClient() {
        int connectTimeout = rheosConfig.getRheosHttpClientConnectTimeout();
        int readTimeout = rheosConfig.getRheosHttpClientReadTimeout();
        RheosHttpClient.RheosHttpClientBuilder httpClientBuilder = new RheosHttpClient.RheosHttpClientBuilder(connectTimeout,
                readTimeout);

        if (rheosConfig.getRhoesHttpClientSSLCheckEnabled()) {
            return httpClientBuilder.enableCustomizedSSLCheck()
                    .keystorePath(rheosConfig.getRheosKeystorePath())
                    .keystorePassword(rheosConfig.getRheosKeystorePassword())
                    .truststorePath(rheosConfig.getRheosTruststorePath())
                    .truststorePassword(rheosConfig.getRheosTruststorePassword()).build();
        } else {
            return httpClientBuilder.disableSSLCheck().build();
        }
    }

    private String getKeyStoneToken(Map<String, String> sourceProperties) throws IOException {
        String url = String.format(Locale.ROOT, "%s/auth", rheosConfig.getRheosManagementURL());
        String userAndPwd = rheosConfig.getSupportKeyStoneApiKey() + ":" + rheosConfig.getSupportKeyStoneApiSecret();
        String base64Auth = "Basic " + Base64.getEncoder().encodeToString(userAndPwd.getBytes(StandardCharsets.UTF_8));

        RheosHttpClient httpClient = getRheosHttpClient();
        Map<String, String> headers = Maps.newHashMap();
        headers.put("Content-Type", "application/json");
        headers.put("x-rheos-namespace", mustGetTopicNamespace(sourceProperties));
        headers.put("x-rheos-tokentype", "api_key");
        headers.put("Authorization", base64Auth);

        String tokenDetails = httpClient.post(url, headers, null, new StringResponseHandler());
        return parseAndGetKeyStoneToken(tokenDetails);
    }

    private boolean isSSLEnabled(String securityMode) {
        try {
            SecurityProtocol securityProtocol = SecurityProtocol.forName(securityMode);
            return SecurityProtocol.SASL_SSL.equals(securityProtocol);
        } catch (IllegalArgumentException e) {
            logger.warn("Fail to get related enum SecurityProtocol for {}", securityMode);
            return false;
        }
    }

    private String mustGetRheosTopicConsumerName(RheosConfig rheosConfig, Map<String, String> sourceProperties) {
        String consumerName = rheosConfig.getRheosConsumerName();
        if (StringUtil.isEmpty(consumerName)) {
            consumerName = getTopicConsumerName(sourceProperties);
        }
        Preconditions.checkNotNull(consumerName, "The consumer name should not be null");
        Preconditions.checkArgument(!consumerName.isEmpty(), "The consumer name should be empty");
        return consumerName;
    }

    private String getTopicConsumerName(Map<String, String> sourceProperties) {
        return sourceProperties.get(PROP_TOPIC_CONSUMER_NAME);
    }


    private String mustGetRheosTopicDC(RheosConfig rheosConfig, Map<String, String> sourceProperties) {
        String dcName = rheosConfig.getRheosDC();
        if (StringUtil.isEmpty(dcName)) {
            dcName = getTopicDC(sourceProperties);
        }
        Preconditions.checkNotNull(dcName, "dcName should not be null");
        Preconditions.checkArgument(!dcName.isEmpty(), "dcName should not be empty");
        return dcName;
    }

    private String getTopicDC(Map<String, String> sourceProperties) {
        return sourceProperties.get(PROP_TOPIC_DC);
    }

    private String mustGetTopicNamespace(Map<String, String> sourceProperties) {
        String namespace = sourceProperties.get(PROP_TOPIC_NAMESPACE);
        Preconditions.checkNotNull(namespace, "namespace should not be null");
        Preconditions.checkArgument(!namespace.isEmpty(), "namespace should not be empty");
        return namespace;
    }

    private String mustGetTopicStream(Map<String, String> sourceProperties) {
        String stream = sourceProperties.get(PROP_TOPIC_STREAM);
        Preconditions.checkNotNull(stream, "topicId should not be null");
        Preconditions.checkArgument(!stream.isEmpty(), "stream should not be empty");
        return stream;
    }

    private String parseAndGetKeyStoneToken(String tokenDetails) throws IOException {
        logger.debug("Token details: {}", tokenDetails);
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode rootNode = objectMapper.readTree(tokenDetails);
        JsonNode tokenDataNode = rootNode.path("data");
        String accessToken = tokenDataNode.path("accessToken").asText();
        String tokenType = tokenDataNode.path("tokenType").asText();
        return tokenType + " " + accessToken;
    }

    private String parseAndGetRheosSecurityMode(String streamDetails) throws IOException {
        logger.debug("Stream details: {}", streamDetails);
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode rootNode = objectMapper.readTree(streamDetails);
        JsonNode schema = rootNode.path("data").path("securityMode");
        return schema.asText();
    }

    private String parseAndGetRheosTopicId(String topicDetails) throws IOException {
        logger.debug("Topic details: {}", topicDetails);
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode rootNode = objectMapper.readTree(topicDetails);
        JsonNode schema = rootNode.path("data").path("id");
        return schema.asText();
    }

    @VisibleForTesting
    static String parseAndGetRheosTopicSubject(String topicSubjectInfo) throws IOException {
        logger.debug("Topic subject info details: {}", topicSubjectInfo);
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode rootNode = objectMapper.readTree(topicSubjectInfo);
        JsonNode subjectListNode = rootNode.get("data");
        if (subjectListNode.isEmpty()) {
            return null;
        }
        JsonNode subjectNode = subjectListNode.get(0).get("subject");
        return subjectNode.asText();
    }

    private String parseAndGetRheosTopicSchemaContent(String topicSchemaDetails) throws IOException {
        logger.debug("Stream content details: {}", topicSchemaDetails);
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode rootNode = objectMapper.readTree(topicSchemaDetails);
        JsonNode schemaNode = rootNode.get("data").get("body");
        return schemaNode.asText();
    }

    private enum EventType {
        RHEOS_EVENT;
    }
}
