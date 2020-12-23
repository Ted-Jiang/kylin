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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.junit.Assert;
import org.junit.Test;


public class MessageTemplateUtilsTest {

    private String avorSchemaForJsonFormat = "{\n" +
            "  \"type\": \"record\",\n" +
            "  \"name\": \"TransPageKylin\",\n" +
            "  \"namespace\": \"misc.crossdcs.trans.page.kylin\",\n" +
            "  \"fields\": [\n" +
            "    {\n" +
            "      \"name\": \"rheosHeader\",\n" +
            "      \"type\": {\n" +
            "        \"type\": \"record\",\n" +
            "        \"name\": \"RheosHeader\",\n" +
            "        \"fields\": [\n" +
            "          {\n" +
            "            \"name\": \"eventCreateTimestamp\",\n" +
            "            \"type\": \"long\"\n" +
            "          },\n" +
            "          {\n" +
            "            \"name\": \"eventSentTimestamp\",\n" +
            "            \"type\": \"long\"\n" +
            "          },\n" +
            "          {\n" +
            "            \"name\": \"schemaId\",\n" +
            "            \"type\": \"int\"\n" +
            "          },\n" +
            "          {\n" +
            "            \"name\": \"eventId\",\n" +
            "            \"type\": [\n" +
            "              \"null\",\n" +
            "              {\n" +
            "                \"type\": \"string\",\n" +
            "                \"avro.java.string\": \"String\"\n" +
            "              }\n" +
            "            ]\n" +
            "          },\n" +
            "          {\n" +
            "            \"name\": \"producerId\",\n" +
            "            \"type\": {\n" +
            "              \"type\": \"string\",\n" +
            "              \"avro.java.string\": \"String\"\n" +
            "            }\n" +
            "          }\n" +
            "        ]\n" +
            "      },\n" +
            "      \"doc\": \"Rheos header\"\n" +
            "    },\n" +
            "    {\n" +
            "      \"name\": \"page_id\",\n" +
            "      \"type\": [\n" +
            "        \"null\",\n" +
            "        \"string\"\n" +
            "      ]\n" +
            "    },\n" +
            "    {\n" +
            "      \"name\": \"pv\",\n" +
            "      \"type\": \"long\"\n" +
            "    },\n" +
            "    {\n" +
            "      \"name\": \"hour\",\n" +
            "      \"type\": [\n" +
            "        \"null\",\n" +
            "        \"int\"\n" +
            "      ]\n" +
            "    }\n" +
            "  ]\n" +
            "}";

    @Test
    public void convertAvroSchemaToJson() {
        // Test for Primitive type
        Schema schema = SchemaBuilder.record("test_record").namespace("test_namespace").fields()
                // INT with nullable
                .name("field1").type().nullable().intType().noDefault()
                // LONG without nullable
                .name("field2").type().nullable().longType().noDefault()
                // FLOAT
                .name("field3").type().floatType().noDefault()
                // DOUBLE
                .name("field4").type().doubleType().noDefault()
                // BOOLEAN
                .name("field5").type().booleanType().noDefault()
                // BYTES
                .name("field6").type().bytesType().noDefault()
                // STRING
                .name("field7").type().stringType().noDefault().endRecord();

        ObjectMapper mapper = new ObjectMapper();
        ObjectNode root = mapper.createObjectNode();
        root.put("field1", (Integer) MessageTemplateUtils.DEFAULT_SAMPLE_VALUES.get(Schema.Type.INT));
        root.put("field2", (Long) MessageTemplateUtils.DEFAULT_SAMPLE_VALUES.get(Schema.Type.LONG));
        root.put("field3", (Float) MessageTemplateUtils.DEFAULT_SAMPLE_VALUES.get(Schema.Type.FLOAT));
        root.put("field4", (Double) MessageTemplateUtils.DEFAULT_SAMPLE_VALUES.get(Schema.Type.DOUBLE));
        root.put("field5", (Boolean) MessageTemplateUtils.DEFAULT_SAMPLE_VALUES.get(Schema.Type.BOOLEAN));
        root.put("field6", (byte[]) MessageTemplateUtils.DEFAULT_SAMPLE_VALUES.get(Schema.Type.BYTES));
        root.put("field7", (String) MessageTemplateUtils.DEFAULT_SAMPLE_VALUES.get(Schema.Type.STRING));
        String expectResult = root.toPrettyString();
        String sampleMessage = MessageTemplateUtils.convertAvroSchemaToJson(schema);
        Assert.assertEquals(expectResult, sampleMessage);

        // Test Rheos Header
        schema = SchemaBuilder.record("test_record").namespace("test_namespace").fields()
                .name("rheosHeader").type()
                .record("RheosHeader").fields()
                    .name("eventCreateTimestamp").type().longType().noDefault()
                    .name("eventSentTimestamp").type().longType().noDefault().name("schemaId").type().intType().noDefault()
                    .name("eventId").type().nullable().stringType().noDefault().name("producerId").type().stringType().noDefault()
                    .endRecord().noDefault()
                .endRecord();

        root = mapper.createObjectNode();
        ObjectNode headerNode = mapper.createObjectNode();
        headerNode.put(MessageTemplateUtils.EVENT_CREATE_TIME, MessageTemplateUtils.TIMESTAMP);
        headerNode.put(MessageTemplateUtils.EVENT_SEND_TIME, MessageTemplateUtils.TIMESTAMP);
        root.set(MessageTemplateUtils.RHEOS_HEADER, headerNode);
        expectResult = root.toPrettyString();
        sampleMessage = MessageTemplateUtils.convertAvroSchemaToJson(schema);
        Assert.assertEquals(expectResult, sampleMessage);

        // Test Rheos Header with other fields
        schema = SchemaBuilder.record("test_record").namespace("test_namespace").fields()
                .name("rheosHeader").type()
                // RECORD for Rheos header
                .record("RheosHeader").fields()
                    .name("eventCreateTimestamp").type().longType().noDefault()
                    .name("eventSentTimestamp").type().longType().noDefault()
                    .name("schemaId").type().intType().noDefault()
                    .name("eventId").type().nullable().stringType().noDefault()
                    .name("producerId").type().stringType().noDefault()
                    .endRecord().noDefault()
                // INT with nullable
                .name("field1").type().nullable().intType().noDefault()
                // LONG without nullable
                .name("field2").type().nullable().longType().noDefault()
                // FLOAT
                .name("field3").type().floatType().noDefault()
                // DOUBLE
                .name("field4").type().doubleType().noDefault()
                // BOOLEAN
                .name("field5").type().booleanType().noDefault()
                // BYTES
                .name("field6").type().bytesType().noDefault()
                // STRING
                .name("field7").type().stringType().noDefault()
                .endRecord();

        root = mapper.createObjectNode();
        headerNode = mapper.createObjectNode();
        headerNode.put(MessageTemplateUtils.EVENT_CREATE_TIME, MessageTemplateUtils.TIMESTAMP);
        headerNode.put(MessageTemplateUtils.EVENT_SEND_TIME, MessageTemplateUtils.TIMESTAMP);
        root.set(MessageTemplateUtils.RHEOS_HEADER, headerNode);

        root.put("field1", (Integer) MessageTemplateUtils.DEFAULT_SAMPLE_VALUES.get(Schema.Type.INT));
        root.put("field2", (Long) MessageTemplateUtils.DEFAULT_SAMPLE_VALUES.get(Schema.Type.LONG));
        root.put("field3", (Float) MessageTemplateUtils.DEFAULT_SAMPLE_VALUES.get(Schema.Type.FLOAT));
        root.put("field4", (Double) MessageTemplateUtils.DEFAULT_SAMPLE_VALUES.get(Schema.Type.DOUBLE));
        root.put("field5", (Boolean) MessageTemplateUtils.DEFAULT_SAMPLE_VALUES.get(Schema.Type.BOOLEAN));
        root.put("field6", (byte[]) MessageTemplateUtils.DEFAULT_SAMPLE_VALUES.get(Schema.Type.BYTES));
        root.put("field7", (String) MessageTemplateUtils.DEFAULT_SAMPLE_VALUES.get(Schema.Type.STRING));

        expectResult = root.toPrettyString();
        sampleMessage = MessageTemplateUtils.convertAvroSchemaToJson(schema);
        Assert.assertEquals(expectResult, sampleMessage);

        // Test avro schema from json-format
        schema = new Schema.Parser().parse(avorSchemaForJsonFormat);
        root = mapper.createObjectNode();
        headerNode = mapper.createObjectNode();
        headerNode.put(MessageTemplateUtils.EVENT_CREATE_TIME, MessageTemplateUtils.TIMESTAMP);
        headerNode.put(MessageTemplateUtils.EVENT_SEND_TIME, MessageTemplateUtils.TIMESTAMP);
        root.set(MessageTemplateUtils.RHEOS_HEADER, headerNode);

        root.put("page_id", (String) MessageTemplateUtils.DEFAULT_SAMPLE_VALUES.get(Schema.Type.STRING));
        root.put("pv", (Long) MessageTemplateUtils.DEFAULT_SAMPLE_VALUES.get(Schema.Type.LONG));
        root.put("hour", (Integer) MessageTemplateUtils.DEFAULT_SAMPLE_VALUES.get(Schema.Type.INT));
        
        expectResult = root.toPrettyString();
        sampleMessage = MessageTemplateUtils.convertAvroSchemaToJson(schema);
        Assert.assertEquals(expectResult, sampleMessage);
    }
}