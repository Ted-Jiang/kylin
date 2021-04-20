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

package org.apache.kylin.engine.spark;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.AbstractApplication;
import org.apache.kylin.common.util.OptionsHelper;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.common.BatchConstants;

import org.apache.spark.sql.SparkSession;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;

/**
 * Execute a batch of spark sql in order, if a sql execution fails, abort and throw an exception,
 * no longer execute the left sqls.
 */
public class SparkSqlBatch extends AbstractApplication implements Serializable {
    private final Logger logger = LoggerFactory.getLogger(SparkSqlBatch.class);
    private Options options;

    public static final Option OPTION_CUBE_NAME = OptionBuilder.withArgName(BatchConstants.ARG_CUBE_NAME)
            .hasArg()
            .isRequired(true)
            .withDescription("Cube Name")
            .create(BatchConstants.ARG_CUBE_NAME);
    public static final Option OPTION_STEP_NAME = OptionBuilder.withArgName(BatchConstants.ARG_BASE64_ENCODED_STEP_NAME)
            .hasArg()
            .isRequired(true)
            .withDescription("Step Name")
            .create(BatchConstants.ARG_BASE64_ENCODED_STEP_NAME);
    public static final Option OPTION_SEGMENT_ID = OptionBuilder.withArgName(BatchConstants.ARG_SEGMENT_ID)
            .hasArg()
            .isRequired(true)
            .withDescription("Cube Segment Id")
            .create(BatchConstants.ARG_SEGMENT_ID);
    public static final Option OPTION_SQL_COUNT = OptionBuilder.withArgName(BatchConstants.ARG_SQL_COUNT)
            .hasArg()
            .isRequired(true)
            .withDescription("Sql count")
            .create(BatchConstants.ARG_SQL_COUNT);
    public static final Option OPTION_METAURL = OptionBuilder.withArgName(BatchConstants.ARG_META_URL)
            .hasArg()
            .isRequired(true)
            .withDescription("self define conf in spark-sql")
            .create(BatchConstants.ARG_META_URL);

    public SparkSqlBatch() {
        options = new Options();
        options.addOption(OPTION_CUBE_NAME);
        options.addOption(OPTION_STEP_NAME);
        options.addOption(OPTION_SEGMENT_ID);
        options.addOption(OPTION_SQL_COUNT);
        options.addOption(OPTION_METAURL);
    }

    @Override
    protected Options getOptions() {
        return options;
    }

    @Override
    protected void execute(OptionsHelper optionsHelper) throws Exception {
        String cubeName = optionsHelper.getOptionValue(OPTION_CUBE_NAME);
        String stepName = base64Decode(optionsHelper.getOptionValue(OPTION_STEP_NAME));
        String segmentId = optionsHelper.getOptionValue(OPTION_SEGMENT_ID);
        String sqlCountStr = optionsHelper.getOptionValue(OPTION_SQL_COUNT);
        String metaURL = optionsHelper.getOptionValue(OPTION_METAURL);

        KylinConfig kylinConfig = AbstractHadoopJob.loadKylinConfigFromHdfs(metaURL);

        logger.info("start execute sql batch job, cubeName: " + cubeName + ", stepName: " +
                stepName + ", segmentId: " + segmentId + ", sqlCount: " + sqlCountStr);

        int sqlCount = Integer.valueOf(sqlCountStr);
        if (sqlCount <= 0) {
            throw new RuntimeException("Count of sqls to execute must be greater than 0, " +
                    "in fact is " + sqlCountStr);
        }

        SparkSession sparkSession = getSparkSession(stepName + " for cube: " +
                cubeName + ", segment " + segmentId, kylinConfig);
        for (int i = 0; i < sqlCount; i++) {
            String argName = BatchConstants.ARG_BASE64_ENCODED_SQL + String.valueOf(i);
            Option optionSqlText = OptionBuilder.withArgName(argName)
                    .hasArg()
                    .isRequired(true)
                    .create(argName);
            String encodedSql = optionsHelper.getOptionValue(optionSqlText);
            String sqlText = base64Decode(encodedSql).trim();
            if (null != sqlText && sqlText.endsWith(";")) {
                sqlText = sqlText.substring(0, sqlText.length() - 1);
            }
            logger.info("execute spark sql: " + sqlText);
            if (i == sqlCount - 1) {
                sparkSession.sql(sqlText).count();
            } else {
                sparkSession.sql(sqlText);
            }
        }
    }

    private SparkSession getSparkSession(String appName, KylinConfig config) {
        SparkSession.Builder builder = SparkSession.builder()
                .appName(appName)
                .enableHiveSupport();

        Map<String, String> sqlConfigOverride = config.getSparkSQLConfigOverride();
        for (Map.Entry<String, String> entry : sqlConfigOverride.entrySet()) {
            logger.info("Override user-defined spark-sql conf, set {}={}.", entry.getKey(), entry.getValue());
            builder.config(entry.getKey(), entry.getValue());
        }
        return builder.getOrCreate();
    }

    private String base64Decode(String str) throws UnsupportedEncodingException {
        return new String(
                Base64.getDecoder().decode(str.getBytes(StandardCharsets.UTF_8)),
                StandardCharsets.UTF_8
        );
    }
}
