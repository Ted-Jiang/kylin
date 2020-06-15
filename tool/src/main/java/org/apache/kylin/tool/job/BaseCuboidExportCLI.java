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

package org.apache.kylin.tool.job;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.AbstractApplication;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.OptionsHelper;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.engine.mr.JobBuilderSupport;
import org.apache.kylin.engine.mr.MRUtil;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.engine.mr.steps.ExportBaseCuboidJob;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;

public class BaseCuboidExportCLI extends AbstractApplication {
    private static final Logger logger = LoggerFactory.getLogger(BaseCuboidExportCLI.class);

    private static final Option OPTION_CUBE = OptionBuilder.withArgName("cube").hasArg().isRequired(true)
            .withDescription("Specify for which cube to export data").create("cube");
    private static final Option OPTION_OUTPUT = OptionBuilder.withArgName("output").hasArg().isRequired(true)
            .withDescription("Specify for output path for the exported data").create("output");
    private static final Option OPTION_TIME_START = OptionBuilder.withArgName("startTime").hasArg().isRequired(false)
            .withDescription("Specify a start time for filtering which segment's data should be exported")
            .create("startTime");
    private static final Option OPTION_TIME_END = OptionBuilder.withArgName("endTime").hasArg().isRequired(false)
            .withDescription("Specify a end time for filtering which segment's data should be exported")
            .create("endTime");
    private static final Option OPTION_DELIMITER = OptionBuilder.withArgName("delimiter").hasArg().isRequired(false)
            .withDescription("Specify the delimiter for the exported column data").create("delimiter");

    private final Options options;

    public BaseCuboidExportCLI() {
        options = new Options();
        options.addOption(OPTION_CUBE);
        options.addOption(OPTION_OUTPUT);
        options.addOption(OPTION_TIME_START);
        options.addOption(OPTION_TIME_END);
        options.addOption(OPTION_DELIMITER);
    }

    protected Options getOptions() {
        return options;
    }

    protected void execute(OptionsHelper optionsHelper) throws Exception {
        String cubeName = optionsHelper.getOptionValue(OPTION_CUBE);
        String outputDir = optionsHelper.getOptionValue(OPTION_OUTPUT);
        long startTime = parseTime(optionsHelper.getOptionValue(OPTION_TIME_START));
        long endTime = parseTime(optionsHelper.getOptionValue(OPTION_TIME_END));
        if (endTime < 0) {
            endTime = Long.MAX_VALUE;
        }
        if (startTime >= endTime) {
            throw new RuntimeException("start time " + startTime + " should be less than end time " + endTime);
        }
        String delimiter = optionsHelper.getOptionValue(OPTION_DELIMITER);

        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        CubeManager cubeManager = CubeManager.getInstance(kylinConfig);
        CubeInstance cubeInstance = cubeManager.getCube(cubeName);
        if (cubeInstance == null) {
            throw new RuntimeException("cannot find cube " + cubeName);
        }
        Configuration hConf = HadoopUtil.getCurrentConfiguration();
        FileSystem fs = FileSystem.get(hConf);
        Path outputDirPath = new Path(outputDir);
        if (!fs.exists(outputDirPath)) {
            logger.info("Path " + outputDir + " does not exist. Will create one");
            fs.mkdirs(outputDirPath);
        } else if (!fs.isDirectory(outputDirPath)) {
            throw new RuntimeException("Path " + outputDir + " exists. But it's not a directory");
        }

        for (CubeSegment cubeSeg : cubeInstance.getSegments(SegmentStatusEnum.READY)) {
            if (!(startTime <= cubeSeg.getTSRange().start.v && endTime >= cubeSeg.getTSRange().end.v)) {
                logger.info("Will skip segment " + cubeSeg.getName() + ", since it's out of the time range ["
                        + startTime + "," + endTime + ")");
                continue;
            }

            JobBuilderSupport jobSupport = new JobBuilderSupport(cubeSeg, "ADMIN");
            String jobName = "Export_Base_Cuboid_Data_for_Cube_" + cubeName + "_Segment_" + cubeSeg.getName();
            String segmentID = cubeSeg.getUuid();
            String inputPath = jobSupport.getCuboidRootPath(cubeSeg) + "*";
            String outputPath = outputDir + "/" + cubeSeg.getName();

            StringBuilder cmd = new StringBuilder();
            jobSupport.appendMapReduceParameters(cmd);
            jobSupport.appendExecCmdParameters(cmd, BatchConstants.ARG_CUBE_NAME, cubeName);
            jobSupport.appendExecCmdParameters(cmd, BatchConstants.ARG_SEGMENT_ID, segmentID);
            jobSupport.appendExecCmdParameters(cmd, BatchConstants.ARG_JOB_NAME, jobName);
            jobSupport.appendExecCmdParameters(cmd, BatchConstants.ARG_INPUT, inputPath);
            jobSupport.appendExecCmdParameters(cmd, BatchConstants.ARG_OUTPUT, outputPath);
            if (!Strings.isNullOrEmpty(delimiter)) {
                jobSupport.appendExecCmdParameters(cmd, BatchConstants.ARG_DATA_EXPORT_DELIMITER, delimiter);
            }

            String[] args = cmd.toString().trim().split("\\s+");
            ExportBaseCuboidJob mrJob = new ExportBaseCuboidJob();
            mrJob.setConf(hConf);
            MRUtil.runMRJob(mrJob, args);
        }
    }

    private long parseTime(String timeStr) {
        long time = -1;
        if (!Strings.isNullOrEmpty(timeStr)) {
            time = Long.parseLong(timeStr);
        }
        return time;
    }

    public static void main(String[] args) {
        BaseCuboidExportCLI cli = new BaseCuboidExportCLI();
        try {
            cli.execute(args);
            System.exit(0);
        } catch (Exception e) {
            logger.error("error start exporting cube data", e);
            System.exit(-1);
        }
    }
}
