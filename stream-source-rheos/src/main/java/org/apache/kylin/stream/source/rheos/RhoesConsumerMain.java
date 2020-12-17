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

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.ebay.rheos.kafka.client.StreamConnectorConfig;
import io.ebay.rheos.schema.avro.GenericRecordDomainDataDecoder;
import io.ebay.rheos.schema.avro.RheosEventDeserializer;
import io.ebay.rheos.schema.event.RheosEvent;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.stream.core.source.StreamingSourceConfig;
import org.apache.kylin.stream.source.kafka.KafkaSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

public class RhoesConsumerMain {

    private static final Logger logger = LoggerFactory.getLogger(RhoesConsumerMain.class);

    public static void main(String[] args) throws Exception {
        if (args.length != 9) {
            throw new IllegalArgumentException(
                    "should have parameters: parserClassName numOfRecords startMode namespace streamName topicName consumerName type [dc, bootstrapServers]");
        }

        String parserClassName = args[0];
        int numOfRecords = Integer.parseInt(args[1]);
        int startMode = Integer.parseInt(args[2]);

        RheosSource rheosSource = new RheosSource();
        Map<String, String> sourceProperties = Maps.newHashMap();
        sourceProperties.put(RheosSource.PROP_TOPIC_NAMESPACE, args[3]);
        sourceProperties.put(RheosSource.PROP_TOPIC_STREAM, args[4]);
        String topicName = args[5];
        sourceProperties.put(KafkaSource.PROP_TOPIC, topicName);
        sourceProperties.put(RheosSource.PROP_TOPIC_CONSUMER_NAME, args[6]);
        int type = Integer.parseInt(args[7]);
        if (type == 0) {
            sourceProperties.put(KafkaSource.PROP_BOOTSTRAP_SERVERS, args[8]);
        } else {
            sourceProperties.put(RheosSource.PROP_TOPIC_DC, args[8]);
        }

        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        Map<String, Object> properties = rheosSource.getKafkaConf(sourceProperties, kylinConfig);
        logger.info("Kafka properties:\n{}", properties);

        // print some sample records
        try (KafkaConsumer<byte[], byte[]> kafkaConsumer = new KafkaConsumer<>(properties)) {
            List<PartitionInfo> partitionInfos = kafkaConsumer.partitionsFor(topicName);
            System.out.println("partition num:" + partitionInfos.size());
            List<TopicPartition> topicPartitions = Lists.transform(partitionInfos,
                    new Function<PartitionInfo, TopicPartition>() {
                        @Override
                        public TopicPartition apply(PartitionInfo input) {
                            return new TopicPartition(topicName, input.partition());
                        }
                    });
            kafkaConsumer.assign(topicPartitions);
            if (startMode == 0) {
                kafkaConsumer.seekToBeginning(topicPartitions);
            } else if (startMode == 1) {
                kafkaConsumer.seekToEnd(topicPartitions);
            }

            RecordValuePrinter printer;
            if (RheosStreamParser.class.getName().equals(parserClassName)) {
                Map<String, Object> config = Maps.newHashMap();
                RheosConfig rheosConfig = rheosSource.getRheosConfig();
                config.put(StreamConnectorConfig.RHEOS_SERVICES_URLS, rheosConfig.getRheosServiceURL());
                config.putAll(rheosConfig.getSSLProps());
                GenericRecordDomainDataDecoder decoder = new GenericRecordDomainDataDecoder(config);
                RheosEventDeserializer deserializer = new RheosEventDeserializer();
                printer = new RecordValuePrinter() {
                    @Override
                    public void print(String topic, byte[] byteValue) {
                        RheosEvent rheosEvent = deserializer.deserialize(topic, byteValue);
                        
                        GenericRecord domainRecord = decoder.decode(rheosEvent);
                        // this is the schema including data ane header information.
                        System.out.println("Event schema: \n" + rheosEvent.getSchema());
                        System.out.println("Event message: \n"+ rheosEvent);
                        System.out.println();
                    }
                };
            } else {
                printer = new RecordValuePrinter() {
                    @Override
                    public void print(String topic, byte[] byteValue) {
                        System.out.println(new String(byteValue, StandardCharsets.UTF_8));
                    }
                };
            }

            int i = 0;
            while (i < numOfRecords) {
                ConsumerRecords<byte[], byte[]> records = kafkaConsumer.poll(100);

                for (ConsumerRecord<byte[], byte[]> record : records) {
                    byte[] value = record.value();
                    System.out.println("record " + i);
                    System.out.println("size of value:" + value.length);
                    printer.print(record.topic(), value);
                    i++;
                }
                Thread.sleep(100L);
            }
        }

        // print schema template
        StreamingSourceConfig sourceConfig = new StreamingSourceConfig();
        sourceConfig.setProperties(sourceProperties);
        String schemaTemplate = rheosSource.getMessageTemplate(sourceConfig);
        System.out.println("Schema template for topic " + topicName + " is:\n" + schemaTemplate);

        // convert the avro schema to sample template
        Schema schema = new Schema.Parser().parse(schemaTemplate);
        String templateMessage = MessageTemplateUtils.convertAvroSchemaToJson(schema);
        System.out.println("Template message for rheos event "+ topicName + " is: \n" + templateMessage);
    }

    private interface RecordValuePrinter {
        void print(String topic, byte[] byteValue);
    }
}
