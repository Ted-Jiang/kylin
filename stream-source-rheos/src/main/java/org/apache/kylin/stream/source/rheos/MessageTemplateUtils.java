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

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Maps;
import org.apache.avro.Schema;
import org.apache.kylin.stream.core.exception.StreamingException;

import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * This class is used to convert other schemas to message template which is json format.
 */
public class MessageTemplateUtils {

    public final static String RHEOS_HEADER = "rheosHeader";
    public final static String EVENT_CREATE_TIME = "eventCreateTimestamp";
    public final static String EVENT_SEND_TIME = "eventSentTimestamp";
    public final static long TIMESTAMP = 1608026193615L;
    private final static byte[] bytes = { 0, 0 };

    public static Map<Schema.Type, Object> DEFAULT_SAMPLE_VALUES = Maps.newHashMap();
    static {
        DEFAULT_SAMPLE_VALUES.put(Schema.Type.STRING, "foo");
        DEFAULT_SAMPLE_VALUES.put(Schema.Type.BYTES, bytes);
        DEFAULT_SAMPLE_VALUES.put(Schema.Type.INT, Integer.MAX_VALUE);
        DEFAULT_SAMPLE_VALUES.put(Schema.Type.LONG, Long.MAX_VALUE);
        DEFAULT_SAMPLE_VALUES.put(Schema.Type.FLOAT, Float.MAX_VALUE);
        DEFAULT_SAMPLE_VALUES.put(Schema.Type.DOUBLE, Double.MAX_VALUE);
        DEFAULT_SAMPLE_VALUES.put(Schema.Type.BOOLEAN, Boolean.TRUE);
    }

    /**
     * Convert the schema which is represented by avro {@link Schema} to Json String
     * @param rheosEventSchema an avro schema
     * @return a message template using json format
     */
    public static String convertAvroSchemaToJson(Schema rheosEventSchema) {
        Schema.Type type = rheosEventSchema.getType();
        if (!Schema.Type.RECORD.equals(type)) {
            throw new StreamingException("root type is not record, which is: " + rheosEventSchema.getType());
        } else {
            List<Schema.Field> fields = rheosEventSchema.getFields();
            ObjectNode root  = JsonNodeFactory.instance.objectNode();
            for (Schema.Field field : fields) {
                switch (field.schema().getType()) {
                case STRING:
                case BYTES:
                case INT:
                case LONG:
                case FLOAT:
                case DOUBLE:
                case BOOLEAN:
                    convertPrimitiveField(field.name(), field.schema(), root);
                    break;
                case UNION:
                    // must be primitive type with nullable
                    // "type": [
                    //        "null",
                    //        "int"
                    //      ]
                    Schema unionSchema = field.schema();
                    if (unionSchema.getTypes().size() == 2
                            && (unionSchema.getTypes().get(0).getType().equals(Schema.Type.NULL)
                                    || unionSchema.getTypes().get(1).getType().equals(Schema.Type.NULL))) {
                        if (unionSchema.getTypes().get(0).getType().equals(Schema.Type.NULL)) {
                            convertPrimitiveField(field.name(), unionSchema.getTypes().get(1), root);
                        } else {
                            convertPrimitiveField(field.name(), unionSchema.getTypes().get(0), root);
                        }
                    } else {
                        throw new StreamingException(
                                "The union schema is not right, which is: " + unionSchema.toString(true));
                    }
                    break;
                case RECORD:
                    // must be rheos header
                    if (RHEOS_HEADER.equals(field.name()) && field.schema().getType().equals(Schema.Type.RECORD)) {
                        convertRheosHeaderSchema(root);
                    } else {
                        throw new StreamingException(String.format(Locale.ROOT,
                                "The name of filed is not \"rheosHeader\" or the schema type is not RECORD, "
                                        + "which field name is %s, schema type is %s",
                                field.name(), field.schema().getType()));
                    }
                    break;
                default:
                    // NULL,MAP,ARRAY,ENUM,FIXED
                    throw new StreamingException("Unsupported the avro type: " + field.schema().getType());
                }
            }
            return root.toPrettyString();
        }
    }

    private static void convertPrimitiveField(String fieldName, Schema primitiveSchema, ObjectNode root) {
        switch (primitiveSchema.getType()) {
        case STRING:
            root.put(fieldName, (String) DEFAULT_SAMPLE_VALUES.get(Schema.Type.STRING));
            break;
        case BYTES:
            root.put(fieldName, (byte[]) DEFAULT_SAMPLE_VALUES.get(Schema.Type.BYTES));
            break;
        case INT:
            root.put(fieldName, (Integer) DEFAULT_SAMPLE_VALUES.get(Schema.Type.INT));
            break;
        case LONG:
            root.put(fieldName, (Long) DEFAULT_SAMPLE_VALUES.get(Schema.Type.LONG));
            break;
        case FLOAT:
            root.put(fieldName, (Float) DEFAULT_SAMPLE_VALUES.get(Schema.Type.FLOAT));
            break;
        case DOUBLE:
            root.put(fieldName, (Double) DEFAULT_SAMPLE_VALUES.get(Schema.Type.DOUBLE));
            break;
        case BOOLEAN:
            root.put(fieldName, (Boolean) DEFAULT_SAMPLE_VALUES.get(Schema.Type.BOOLEAN));
            break;
        default:
            throw new StreamingException("type is not primitive type, type: " + primitiveSchema.getType());
        }
    }

    private static void convertRheosHeaderSchema(ObjectNode root) {
        // rheosHeader must be RECORD schema
        ObjectNode headerNode = JsonNodeFactory.instance.objectNode();
        headerNode.put(EVENT_CREATE_TIME, TIMESTAMP);
        headerNode.put(EVENT_SEND_TIME, TIMESTAMP);
        root.set(RHEOS_HEADER, headerNode);
    }
}
