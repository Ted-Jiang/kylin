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

package org.apache.kylin.tool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.restclient.RestClient;
import org.apache.kylin.common.util.AbstractApplication;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.MailService;
import org.apache.kylin.common.util.MailTemplateProvider;
import org.apache.kylin.common.util.OptionsHelper;
import org.apache.kylin.common.util.ToolUtil;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.cuboid.algorithm.OptimizationBenefit;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.project.ProjectManager;
import org.apache.kylin.metadata.project.RealizationEntry;
import org.apache.kylin.metadata.realization.RealizationType;
import org.apache.kylin.shaded.com.google.common.base.Joiner;
import org.apache.kylin.shaded.com.google.common.collect.Lists;
import org.apache.kylin.shaded.com.google.common.collect.Maps;
import org.apache.kylin.shaded.com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;

public class CubeOptimizationCLI extends AbstractApplication {
    private static final Logger logger = LoggerFactory.getLogger(CubeOptimizationCLI.class);

    @SuppressWarnings("static-access")
    private static final Option OPTION_KYLIN_CONFIG_URI = OptionBuilder.withArgName("kylinConfigUri").hasArg()
            .isRequired(true).withDescription("Kylin configuration uri for finding related cubes")
            .create("kylinConfigUri");
    @SuppressWarnings("static-access")
    private static final Option OPTION_USER = OptionBuilder.withArgName("user").hasArg().isRequired(true)
            .withDescription("user for authentication").create("user");
    @SuppressWarnings("static-access")
    private static final Option OPTION_PASSWORD = OptionBuilder.withArgName("password").hasArg().isRequired(true)
            .withDescription("password for authentication").create("password");
    @SuppressWarnings("static-access")
    public static final Option OPTION_All = OptionBuilder.withArgName("all").hasArg(false).isRequired(false)
            .withDescription("check all of the cubes' optimization benefit").create("all");
    @SuppressWarnings("static-access")
    private static final Option OPTION_PROJECT = OptionBuilder.withArgName("project").hasArg().isRequired(false)
            .withDescription("check all of the cubes' optimization benefit in this project").create("project");
    @SuppressWarnings("static-access")
    private static final Option OPTION_CUBE = OptionBuilder.withArgName("cube").hasArg().isRequired(false)
            .withDescription("check the optimization benefit for this cube").create("cube");
    @SuppressWarnings("static-access")
    private static final Option OPTION_THRESHOLD = OptionBuilder.withArgName("threshold").hasArg().isRequired(false)
            .withDescription("threshold of score").create("threshold");
    @SuppressWarnings("static-access")
    private static final Option OPTION_TOP_NUM = OptionBuilder.withArgName("topNum").hasArg().isRequired(false)
            .withDescription("number of display cube item which order by total benefit").create("topNum");
    @SuppressWarnings("static-access")
    private static final Option OPTION_TO_OWNER = OptionBuilder.withArgName("toOwner").hasArg().isRequired(false)
            .withDescription("whether send email to owner").create("toOwner");

    private static final String OPTIMIZATION_BENEFITS = "OPTIMIZATION_BENEFITS";

    private KylinConfig kylinConfig;
    private ProjectManager projectManager;
    private CubeManager cubeManager;
    private RestClient client;
    private double threshold;
    private int topNum;
    private boolean ifToOwner;

    private Map<String, Set<CubeInstance>> cubes = Maps.newHashMap();

    protected Options getOptions() {
        OptionGroup cubeOrProject = new OptionGroup();
        cubeOrProject.addOption(OPTION_CUBE);
        cubeOrProject.addOption(OPTION_PROJECT);
        cubeOrProject.addOption(OPTION_All);
        cubeOrProject.setRequired(true);

        Options options = new Options();
        options.addOption(OPTION_KYLIN_CONFIG_URI);
        options.addOption(OPTION_USER);
        options.addOption(OPTION_PASSWORD);
        options.addOptionGroup(cubeOrProject);
        options.addOption(OPTION_THRESHOLD);
        options.addOption(OPTION_TOP_NUM);
        options.addOption(OPTION_TO_OWNER);
        return options;
    }

    protected void init(OptionsHelper optionsHelper) throws Exception {
        String kylinConfigUri = optionsHelper.getOptionValue(OPTION_KYLIN_CONFIG_URI);
        kylinConfig = KylinConfig.createInstanceFromUri(kylinConfigUri);
        projectManager = ProjectManager.getInstance(kylinConfig);
        cubeManager = CubeManager.getInstance(kylinConfig);

        String user = optionsHelper.getOptionValue(OPTION_USER);
        String password = optionsHelper.getOptionValue(OPTION_PASSWORD);
        client = new RestClient(user + ":" + password + "@" + kylinConfigUri);

        threshold = optionsHelper.hasOption(OPTION_THRESHOLD)
                ? Double.parseDouble(optionsHelper.getOptionValue(OPTION_THRESHOLD))
                : 60;

        topNum = optionsHelper.hasOption(OPTION_TOP_NUM) ? Integer.valueOf(optionsHelper.getOptionValue(OPTION_TOP_NUM))
                : 10;

        ifToOwner = optionsHelper.hasOption(OPTION_TO_OWNER)
                && Boolean.parseBoolean(optionsHelper.getOptionValue(OPTION_TO_OWNER));

        if (optionsHelper.hasOption(OPTION_All)) {
            List<ProjectInstance> projects = projectManager.listAllProjects();
            for (ProjectInstance projectEntry : projects) {
                addCubesInProject(projectEntry);
            }
        } else if (optionsHelper.hasOption(OPTION_PROJECT)) {
            Set<String> projectNames = Sets.newHashSet(optionsHelper.getOptionValue(OPTION_PROJECT).split(","));
            for (String projectName : projectNames) {
                ProjectInstance project = projectManager.getProject(projectName);
                if (project == null) {
                    throw new IllegalArgumentException("No project found with name of " + projectName);
                }
                addCubesInProject(project);
            }
        } else if (optionsHelper.hasOption(OPTION_CUBE)) {
            String cubeNames = optionsHelper.getOptionValue(OPTION_CUBE);
            for (String cubeName : cubeNames.split(",")) {
                CubeInstance cube = cubeManager.getCube(cubeName);
                if (cube == null) {
                    throw new IllegalArgumentException("No cube found with name of " + cubeName);
                } else {
                    List<ProjectInstance> projects = projectManager.findProjects(RealizationType.CUBE, cubeName);
                    if (projects.isEmpty()) {
                        throw new IllegalArgumentException("No project contains cube " + cubeName);
                    }
                    addCubeInProject(projects.get(0), cube);
                }
            }
        }
    }

    private void addCubesInProject(ProjectInstance projectEntry) {
        List<RealizationEntry> cubeEntries = projectEntry.getRealizationEntries(RealizationType.CUBE);
        for (RealizationEntry cubeEntry : cubeEntries) {
            CubeInstance cubeInstance = cubeManager.getCube(cubeEntry.getRealization());
            if (cubeInstance == null) {
                logger.warn("Fail to find cube instance {}", cubeEntry.getRealization());
                continue;
            }
            addCubeInProject(projectEntry, cubeInstance);
        }
    }

    private void addCubeInProject(ProjectInstance projectEntry, CubeInstance cubeInstance) {
        Set<CubeInstance> cubeSet = cubes.get(projectEntry.getName());
        if (cubeSet == null) {
            cubeSet = Sets.newHashSet();
        }
        cubeSet.add(cubeInstance);
        cubes.put(projectEntry.getName(), cubeSet);
    }

    protected void execute(OptionsHelper optionsHelper) throws Exception {
        init(optionsHelper);

        if (cubes.isEmpty()) {
            logger.warn("no cubes found for optimization benefit check");
            return;
        }

        Map<CubeInstance, CubeOptBenefit> optBenefitMap = Maps.newHashMap();
        for (Map.Entry<String, Set<CubeInstance>> projectEntry : cubes.entrySet()) {
            for (CubeInstance cubeInstance : projectEntry.getValue()) {
                try {
                    String restContent = client.getCuboidRecommendResponse(cubeInstance.getName());
                    JsonNode jsonNode = JsonUtil.readValueAsTree(restContent);
                    JsonNode optBntNode = jsonNode.get("optBenefit");
                    OptimizationBenefit optBenefit = JsonUtil.readValue(optBntNode.toString(),
                            OptimizationBenefit.class);
                    if (optBenefit.getTotalBenefit() >= threshold) {
                        optBenefitMap.put(cubeInstance,
                                new CubeOptBenefit(projectEntry.getKey(), cubeInstance.getName(), optBenefit));
                    } else {
                        logger.info("Will not optimize cube {} since its total benefit {} is lower than {}",
                                cubeInstance.getName(), optBenefit.getTotalBenefit(), threshold);
                    }
                } catch (Exception e) {
                    logger.warn("fail to get optimization benefit for cube {} due to " + e, cubeInstance.getName());
                }
            }
        }

        List<Map.Entry<CubeInstance, CubeOptBenefit>> optBenefitList = new ArrayList<>(optBenefitMap.entrySet());
        Collections.sort(optBenefitList, new Comparator<Map.Entry<CubeInstance, CubeOptBenefit>>() {
            @Override
            public int compare(Map.Entry<CubeInstance, CubeOptBenefit> o1, Map.Entry<CubeInstance, CubeOptBenefit> o2) {
                return o1.getValue().compareTo(o2.getValue());
            }
        });

        if (optBenefitList.size() > topNum) {
            optBenefitList = optBenefitList.subList(0, topNum);
        }
        
        if (optBenefitList.isEmpty()) {
            logger.info("Don't find any cubes that should be optimized");
            return;
        }

        { // Send to admins
            List<CubeOptBenefit> cubeOptBenefitList = optBenefitList.stream().map(Map.Entry::getValue)
                    .collect(Collectors.toList());
            Map<String, Object> rootMap = Maps.newHashMap();
            List<Map<String, Object>> cubeBenefitList = getMailContent(cubeOptBenefitList);
            rootMap.put("cubeBenefitList", cubeBenefitList);
            String[] receiver = kylinConfig.getAdminDls();
            sendMail(OPTIMIZATION_BENEFITS, rootMap, Lists.newArrayList(receiver));
        }

        if (ifToOwner) {
            // Collect info for each cube owner
            Map<String, List<CubeOptBenefit>> ownerOptBenefitMap = Maps.newHashMap();
            for (Map.Entry<CubeInstance, CubeOptBenefit> entry : optBenefitList) {
                String owner = entry.getKey().getOwner();
                List<CubeOptBenefit> value = ownerOptBenefitMap.get(owner);
                if (value == null) {
                    value = Lists.newArrayList();
                }
                value.add(entry.getValue());
                ownerOptBenefitMap.put(owner, value);
            }

            for (Map.Entry<String, List<CubeOptBenefit>> entry : ownerOptBenefitMap.entrySet()) {
                Map<String, Object> rootMap = Maps.newHashMap();
                List<Map<String, Object>> cubeBenefitList = getMailContent(entry.getValue());
                rootMap.put("cubeBenefitList", cubeBenefitList);
                String receiver = getMailAddress(entry.getKey());
                sendMail(OPTIMIZATION_BENEFITS, rootMap, Lists.newArrayList(receiver));
            }
        }
    }

    private static List<Map<String, Object>> getMailContent(List<CubeOptBenefit> cubeOptBenefitList) {
        List<Map<String, Object>> result = Lists.newArrayList();
        for (CubeOptBenefit cubeOptBenefit : cubeOptBenefitList) {
            Map<String, Object> resultEntry = Maps.newHashMap();
            resultEntry.put("server", ToolUtil.getHostName());
            resultEntry.put("projectName", cubeOptBenefit.project);
            resultEntry.put("cube", cubeOptBenefit.cube);
            OptimizationBenefit optBenefit = cubeOptBenefit.optimizationBenefit;
            resultEntry.put("score", optBenefit.getScore());
            resultEntry.put("totalBenefit", optBenefit.getTotalBenefit());
            resultEntry.put("queryBenefit", optBenefit.getQueryBenefit());
            resultEntry.put("spaceBenefit", optBenefit.getSpaceBenefit());
            resultEntry.put("rollupBenefit", optBenefit.getRollupBenefit());
            resultEntry.put("rollupCost", optBenefit.getRollupCost());
            resultEntry.put("rollupInputCount", optBenefit.getRollupInputCount());
            resultEntry.put("curTotalSize", optBenefit.getCurTotalSize());
            resultEntry.put("recomTotalSize", optBenefit.getRecomTotalSize());
            resultEntry.put("spaceLimit", optBenefit.getSpaceLimit());
            resultEntry.put("k", optBenefit.getK());
            resultEntry.put("scoreHint", optBenefit.getScoreHint());

            result.add(resultEntry);
        }
        return result;
    }

    private static String getMailTitle(String... titleParts) {
        return "[" + Joiner.on("]-[").join(titleParts) + "]";
    }

    private String getMailAddress(String account) {
        return account + kylinConfig.getNotificationMailSuffix();
    }

    private void sendMail(String state, Map<String, Object> root, List<String> emailAddress) {
        String content = MailTemplateProvider.getInstance().buildMailContent(state, root);
        String title = getMailTitle("RECOMMEND", "CUBE_OPTIMIZATION", kylinConfig.getDeployEnv());
        new MailService(kylinConfig).sendMail(emailAddress, title, content);
    }

    public static class CubeOptBenefit implements Comparable<CubeOptBenefit> {
        public final OptimizationBenefit optimizationBenefit;
        public final String project;
        public final String cube;

        public CubeOptBenefit(String project, String cube, OptimizationBenefit optimizationBenefit) {
            this.optimizationBenefit = optimizationBenefit;
            this.project = project;
            this.cube = cube;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            CubeOptBenefit that = (CubeOptBenefit) o;
            return Objects.equals(project, that.project) && Objects.equals(cube, that.cube);
        }

        @Override
        public int hashCode() {
            return Objects.hash(project, cube);
        }

        @Override
        public int compareTo(CubeOptBenefit o) {
            if (!this.equals(o)) {
                if (optimizationBenefit.getTotalBenefit() > o.optimizationBenefit.getTotalBenefit()) {
                    return -1;
                } else if (optimizationBenefit.getTotalBenefit() < o.optimizationBenefit.getTotalBenefit()) {
                    return 1;
                } else if (project.compareTo(o.project) < 0) {
                    return -1;
                } else if (project.compareTo(o.project) > 0) {
                    return 1;
                } else if (cube.compareTo(o.cube) < 0) {
                    return -1;
                } else if (cube.compareTo(o.cube) > 0) {
                    return 1;
                }
            }
            return 0;
        }
    }

    public static void main(String[] args) throws IOException {
        new CubeOptimizationCLI().execute(args);
    }
}
