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

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.util.OptionsHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;

public class ECubeMigrationCrossClusterCLI extends CubeMigrationCrossClusterCLI {

    private static final Logger logger = LoggerFactory.getLogger(ECubeMigrationCrossClusterCLI.class);

    @SuppressWarnings("static-access")
    public static final Option OPTION_DISTCP_DO_AS_NAME = OptionBuilder.withArgName("distCpDoAsName").hasArg()
            .isRequired(true).withDescription("Specify do as name for distCp").create("distCpDoAsName");

    @SuppressWarnings("static-access")
    public static final Option OPTION_DISTCP_REST_URL = OptionBuilder.withArgName("distCpRestUrl").hasArg()
            .isRequired(true).withDescription("Specify rest url for distCp").create("distCpRestUrl");

    @SuppressWarnings("static-access")
    public static final Option OPTION_DISTCP_TOKEN_URL = OptionBuilder.withArgName("distCpTokenUrl").hasArg()
            .isRequired(true).withDescription("Specify rest url for asking token for distCp").create("distCpTokenUrl");

    @SuppressWarnings("static-access")
    public static final Option OPTION_DISTCP_TOKEN_KEY = OptionBuilder.withArgName("distCpTokenKey").hasArg()
            .isRequired(true).withDescription("Specify api key for asking token").create("distCpTokenKey");

    @SuppressWarnings("static-access")
    public static final Option OPTION_DISTCP_TOKEN_SECRET = OptionBuilder.withArgName("distCpTokenSecret").hasArg()
            .isRequired(true).withDescription("Specify api secret for asking token").create("distCpTokenSecret");

    @SuppressWarnings("static-access")
    public static final Option OPTION_DISTCP_FILE_ATTR_KEPT = OptionBuilder.withArgName("distCpFileAttrKept").hasArg()
            .isRequired(true).withDescription("Specify the distCp fileAttrKept code").create("distCpFileAttrKept");

    @SuppressWarnings("static-access")
    public static final Option OPTION_DISTCP_JOB_MAX_DURATION = OptionBuilder.withArgName("distCpJobMaxDuration")
            .hasArg().isRequired(false).withDescription("Specify distCp job max duration")
            .create("distCpJobMaxDuration");
    @SuppressWarnings("static-access")
    public static final Option OPTION_DISTCP_JOB_STATUS_CHECK_INTERVAL = OptionBuilder
            .withArgName("distCpJobCheckInterval").hasArg().isRequired(false)
            .withDescription("Specify distCp job status check interval(minute unit)").create("distCpJobCheckInterval");

    @SuppressWarnings("static-access")
    public static final Option OPTION_FS_ACLS_ENABLED_CODE = OptionBuilder.withArgName("codeOfFSAclsEnabled").hasArg()
            .isRequired(false).withDescription("Specify whether dfs.namenode.acls.enabled is true or false")
            .create("codeOfFSAclsEnabled");

    private static final String KeyOfFsAclsEnabled = "dfs.namenode.acls.enabled";
    private static final String KeyOfIpcClientSocketBind = "hadoop.ebay.ipc.client.socket.bind";

    private static final long DEFAULT_DISTCP_JOB_MAX_DURATION = 120L;
    private static final long DEFAULT_DISTCP_JOB_STATUS_CHECK_INTERVAL = 1L;

    protected EDistCpRestClient distCpClient;

    private int codeOfFSAclsEnabled = 3;

    public ECubeMigrationCrossClusterCLI() {
        super();

        options.addOption(OPTION_DISTCP_JOB_MAX_DURATION);
        options.addOption(OPTION_DISTCP_JOB_STATUS_CHECK_INTERVAL);
        options.addOption(OPTION_DISTCP_REST_URL);
        options.addOption(OPTION_FS_ACLS_ENABLED_CODE);
        options.addOption(OPTION_DISTCP_TOKEN_URL);
        options.addOption(OPTION_DISTCP_TOKEN_KEY);
        options.addOption(OPTION_DISTCP_TOKEN_SECRET);
        options.addOption(OPTION_DISTCP_DO_AS_NAME);
        options.addOption(OPTION_DISTCP_FILE_ATTR_KEPT);
    }

    public static boolean ifFSAclsEnabled(int code, int pos) {
        int which = 1 << pos;
        return (code & which) == which;
    }

    @Override
    protected void init(OptionsHelper optionsHelper) throws Exception {
        super.init(optionsHelper);

        codeOfFSAclsEnabled = optionsHelper.hasOption(OPTION_FS_ACLS_ENABLED_CODE)
                ? Integer.parseInt(optionsHelper.getOptionValue(OPTION_FS_ACLS_ENABLED_CODE))
                : 3;
        srcCluster.jobConf.set(KeyOfFsAclsEnabled, "" + ifFSAclsEnabled(codeOfFSAclsEnabled, 0));
        srcCluster.hbaseConf.set(KeyOfFsAclsEnabled, "" + ifFSAclsEnabled(codeOfFSAclsEnabled, 1));
        srcCluster.jobConf.set(KeyOfIpcClientSocketBind, "false");

        long distCpJobMaxDuration = optionsHelper.hasOption(OPTION_DISTCP_JOB_MAX_DURATION)
                ? Long.parseLong(optionsHelper.getOptionValue(OPTION_DISTCP_JOB_MAX_DURATION))
                : DEFAULT_DISTCP_JOB_MAX_DURATION;

        long distCpJobCheckInterval = optionsHelper.hasOption(OPTION_DISTCP_JOB_STATUS_CHECK_INTERVAL)
                ? Long.parseLong(optionsHelper.getOptionValue(OPTION_DISTCP_JOB_STATUS_CHECK_INTERVAL))
                : DEFAULT_DISTCP_JOB_STATUS_CHECK_INTERVAL;

        String distCpRestUrl = optionsHelper.getOptionValue(OPTION_DISTCP_REST_URL);
        String distCpTokenUrl = optionsHelper.getOptionValue(OPTION_DISTCP_TOKEN_URL);
        String distCpTokenKey = optionsHelper.getOptionValue(OPTION_DISTCP_TOKEN_KEY);
        String distCpTokenSecret = optionsHelper.getOptionValue(OPTION_DISTCP_TOKEN_SECRET);
        String distCpDoAsName = optionsHelper.getOptionValue(OPTION_DISTCP_DO_AS_NAME);
        int fileAttrKept = Integer.parseInt(optionsHelper.getOptionValue(OPTION_DISTCP_FILE_ATTR_KEPT));
        distCpClient = new EDistCpRestClient(distCpRestUrl, distCpJobMaxDuration, distCpJobCheckInterval,
                distCpTokenUrl, distCpTokenKey, distCpTokenSecret, distCpDoAsName, fileAttrKept, nThread);
    }

    @Override
    protected void copyHDFSPath(String srcDir, Configuration srcConf, String dstDir, Configuration dstConf)
            throws Exception {
        Path dstPathParent = new Path(dstDir).getParent();
        FileSystem dstFs = FileSystem.get(dstConf);
        if (!dstFs.exists(dstPathParent)) {
            dstFs.mkdirs(dstPathParent);
        }

        logger.info("start to copy hdfs directory from {} to {}", srcDir, dstDir);
        String aclsEnabledStr = srcConf.get(KeyOfFsAclsEnabled);
        boolean aclsEnabled = Strings.isNullOrEmpty(aclsEnabledStr) || Boolean.parseBoolean(aclsEnabledStr);
        logger.info("{}: {}", KeyOfFsAclsEnabled, aclsEnabled);
        String jobId = distCpClient.submitDistCpJob(srcDir, dstPathParent.toString(), aclsEnabled);
        distCpClient.waitForJobFinish(jobId);
        logger.info("copied hdfs directory from {} to {}", srcDir, dstDir);
    }

    public static void main(String[] args) {
        ECubeMigrationCrossClusterCLI cli = new ECubeMigrationCrossClusterCLI();
        cli.execute(args);
    }
}
