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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.ebay.rheos.kafka.client.StreamConnectorConfig;
import io.ebay.rheos.schema.avro.GenericRecordDomainDataDecoder;
import io.ebay.rheos.schema.avro.RheosEventDeserializer;
import io.ebay.rheos.schema.event.RheosEvent;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kylin.common.util.DateFormat;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.CubeJoinedFlatTableDesc;
import org.apache.kylin.dimension.TimeDerivedColumnType;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.stream.core.model.StreamingMessage;
import org.apache.kylin.stream.core.source.IStreamingMessageParser;
import org.apache.kylin.stream.core.source.MessageFormatException;
import org.apache.kylin.stream.core.source.MessageParserInfo;
import org.apache.kylin.stream.source.kafka.KafkaPosition.KafkaPartitionPosition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.apache.kylin.common.util.DateFormat.DEFAULT_DATETIME_PATTERN_WITHOUT_MILLISECONDS;
import static org.apache.kylin.common.util.DateFormat.DEFAULT_DATETIME_PATTERN_WITH_MILLISECONDS;

/**
 * Rhoes event parser
 */
public class RheosStreamParser implements IStreamingMessageParser<ConsumerRecord<byte[], byte[]>> {

    private static final Logger logger = LoggerFactory.getLogger(RheosStreamParser.class);

    protected List<TblColRef> allColumns;
    private boolean formatTs = false;// not used
    private String tsColName = "TIMESTAMP";
    protected TimeColumnFormat timeColumnFormat = null;

    protected RheosEventDeserializer deserializer;
    protected GenericRecordDomainDataDecoder decoder;
    protected Map<String, String[]> columnToSourceFieldMapping;

    public RheosStreamParser(CubeDesc cubeDesc, MessageParserInfo parserInfo) {
        this.allColumns = new CubeJoinedFlatTableDesc(cubeDesc).getAllColumns();
        this.deserializer = new RheosEventDeserializer();
        Map<String, Object> config = Maps.newHashMap();
        RheosConfig rheosConfig = new RheosConfig(cubeDesc.getConfig());
        String rheosServiceUrl = rheosConfig.getRheosServiceURL();
        config.put(StreamConnectorConfig.RHEOS_SERVICES_URLS, rheosServiceUrl);
        config.putAll(rheosConfig.getSSLProps());

        this.decoder = new GenericRecordDomainDataDecoder(config);
        Map<String, String> columnMappings = null;
        if (parserInfo != null) {
            this.formatTs = parserInfo.isFormatTs();
            this.tsColName = parserInfo.getTsColName().toUpperCase(Locale.ROOT);
            columnMappings = parserInfo.getColumnToSourceFieldMapping();
            this.columnToSourceFieldMapping = Maps.newHashMap();
            if (columnMappings != null) {
                for (Map.Entry<String, String> mappingEntry : columnMappings.entrySet()) {
                    String[] hierarchies = mappingEntry.getValue().split("\\.");
                    columnToSourceFieldMapping.put(mappingEntry.getKey().toUpperCase(Locale.ROOT), hierarchies);
                }
            }
        }

        logger.info("RheosStreamParser with formatTs {}, tsColName {}, columnMappings {}", formatTs, tsColName,
                columnMappings);
    }

    public StreamingMessage parse(ConsumerRecord<byte[], byte[]> record) {
        GenericRecord domainRecord = null;
        try {
            RheosEvent rheosEvent = deserializer.deserialize(record.topic(), record.value());
            domainRecord = decoder.decode(rheosEvent);
            long t = parseTimeColumn(domainRecord);
            List<String> result = Lists.newArrayList();

            for (TblColRef column : allColumns) {
                String columnName = column.getName();
                TimeDerivedColumnType columnType = TimeDerivedColumnType.getTimeDerivedColumnType(columnName);
                if (columnType != null) {
                    result.add(String.valueOf(columnType.normalize(t)));
                } else {
                    Object colValue = getColumnValue(domainRecord, columnName);
                    if (colValue != null) {
                        result.add(colValue.toString());
                    } else {
                        result.add(null);
                    }
                }
            }
            return new StreamingMessage(result, new KafkaPartitionPosition(record.partition(), record.offset()), t,
                    Collections.<String, Object>emptyMap());
        } catch (Exception e) {
            String msg = "invalid record:" + (domainRecord == null ? "null" : domainRecord.toString());
            throw new MessageFormatException(msg, e);
        }
    }

    private long parseTimeColumn(GenericRecord domainRecord) {
        Object tsObj = getColumnValue(domainRecord, tsColName);
        String tsStr = (tsObj == null ? null : tsObj.toString());
        if (StringUtils.isEmpty(tsStr)) {
            throw new MessageFormatException("time column is empty for the record");
        }

        if (timeColumnFormat == null) {
            timeColumnFormat = TimeColumnFormat.getTimeColumnFormat(tsStr);
            logger.info("time column format is:{}", timeColumnFormat);
        }
        return timeColumnFormat.parseTime(tsStr);
    }

    private Object getColumnValue(GenericRecord domainRecord, String colName) {
        if (columnToSourceFieldMapping == null || !columnToSourceFieldMapping.containsKey(colName)) {
            return domainRecord.get(colName);
        }

        String[] hierarchies = columnToSourceFieldMapping.get(colName);
        if (hierarchies.length == 0) {
            return null;
        }
        GenericRecord directParent = domainRecord;
        for (int i = 0; i < hierarchies.length - 1; i++) {
            directParent = (GenericRecord) directParent.get(hierarchies[i]);
            if (directParent == null) {
                return null;
            }
        }
        return directParent.get(hierarchies[hierarchies.length - 1]);
    }

    public boolean filter(StreamingMessage streamingMessage) {
        return true;
    }

    public static class TimeColumnFormat {
        private boolean isLongFormat;
        private String timeFormat;

        public static final String[] SUPPORTED_TIME_PATTERN = { //
                DEFAULT_DATETIME_PATTERN_WITHOUT_MILLISECONDS, //
                DEFAULT_DATETIME_PATTERN_WITH_MILLISECONDS};

        public static TimeColumnFormat getTimeColumnFormat(String sampleDateVal) {
            TimeColumnFormat result = new TimeColumnFormat();
            try {
                Long.parseLong(sampleDateVal);
                result.setLongFormat(true);
            } catch (Exception e) {
                String timeFormat = null;
                for (String formatStr : SUPPORTED_TIME_PATTERN) {
                    try {
                        DateFormat.stringToDate(sampleDateVal, formatStr);
                        timeFormat = formatStr;
                        break;
                    } catch (Exception ex) {
                        continue;
                    }
                }
                if (timeFormat == null) {
                    throw new IllegalStateException("unRecognized time column format: " + sampleDateVal);
                } else {
                    result.setTimeFormat(timeFormat);
                }
            }
            return result;
        }

        public boolean isLongFormat() {
            return isLongFormat;
        }

        public void setLongFormat(boolean longFormat) {
            isLongFormat = longFormat;
        }

        public String getTimeFormat() {
            return timeFormat;
        }

        public void setTimeFormat(String timeFormat) {
            this.timeFormat = timeFormat;
        }

        public long parseTime(String timeVal) {
            if (isLongFormat) {
                return Long.valueOf(timeVal);
            }
            return DateFormat.stringToDate(timeVal, timeFormat).getTime();
        }

        @Override
        public String toString() {
            return "TimeColumnFormat{" + "isLongFormat=" + isLongFormat + ", timeFormat='" + timeFormat + '\'' + '}';
        }
    }

}
