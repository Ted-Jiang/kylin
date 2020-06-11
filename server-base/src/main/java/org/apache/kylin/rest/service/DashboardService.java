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

package org.apache.kylin.rest.service;

import java.util.List;

import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.realization.RealizationType;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.exception.BadRequestException;
import org.apache.kylin.rest.request.PrepareSqlRequest;
import org.apache.kylin.rest.response.MetricsResponse;
import org.apache.kylin.rest.response.SQLResponse;
import org.apache.kylin.rest.util.SqlCreationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.stereotype.Component;

@Component("dashboardService")
public class DashboardService extends BasicService {

    private static final Logger logger = LoggerFactory.getLogger(DashboardService.class);

    @Autowired
    private CubeService cubeService;

    @Autowired
    private QueryService queryService;

    public MetricsResponse getCubeMetrics(String projectName, String cubeName) {
        MetricsResponse cubeMetrics = new MetricsResponse();
        Float totalCubeSize = 0f;
        long totalRecoadSize = 0;
        List<CubeInstance> cubeInstances = cubeService.listAllCubes(cubeName, projectName, null, true);
        Integer totalCube = cubeInstances.size();
        if (projectName == null) {
            totalCube += getHybridManager().listHybridInstances().size();
        } else {
            ProjectInstance project = getProjectManager().getProject(projectName);
            totalCube += project.getRealizationCount(RealizationType.HYBRID);
        }
        Float minCubeExpansion = Float.POSITIVE_INFINITY;
        Float maxCubeExpansion = Float.NEGATIVE_INFINITY;
        cubeMetrics.increase("totalCube", totalCube.floatValue());
        for (CubeInstance cubeInstance : cubeInstances) {
            if (cubeInstance.getInputRecordSizeBytes() > 0) {
                totalCubeSize += cubeInstance.getSizeKB();
                totalRecoadSize += cubeInstance.getInputRecordSizeBytes();
                Float cubeExpansion = new Float(cubeInstance.getSizeKB()) * 1024
                        / cubeInstance.getInputRecordSizeBytes();
                if (cubeExpansion > maxCubeExpansion) {
                    maxCubeExpansion = cubeExpansion;
                }
                if (cubeExpansion < minCubeExpansion) {
                    minCubeExpansion = cubeExpansion;
                }
            }
        }
        Float avgCubeExpansion = 0f;
        if (totalRecoadSize != 0) {
            avgCubeExpansion = totalCubeSize * 1024 / totalRecoadSize;
        }
        cubeMetrics.increase("avgCubeExpansion", avgCubeExpansion);
        cubeMetrics.increase("maxCubeExpansion", maxCubeExpansion == Float.NEGATIVE_INFINITY ? 0 : maxCubeExpansion);
        cubeMetrics.increase("minCubeExpansion", minCubeExpansion == Float.POSITIVE_INFINITY ? 0 : minCubeExpansion);
        return cubeMetrics;
    }

    public MetricsResponse getQueryMetrics(String startTime, String endTime, String projectName, String cubeName) {
        PrepareSqlRequest sqlRequest = SqlCreationUtil.createPrepareSqlRequestOfTotalQueryMetrics(startTime, endTime,
                projectName, cubeName);
        SQLResponse sqlResponse = queryService.doQueryWithCache(sqlRequest, false);

        MetricsResponse queryMetrics = new MetricsResponse();
        if (!sqlResponse.getIsException()) {
            List<String> row = sqlResponse.getResults().get(0);
            queryMetrics.increase("queryCount", getMetricValue(row.get(0)));
            queryMetrics.increase("avgQueryLatency", getMetricValue(row.get(1)));
            queryMetrics.increase("maxQueryLatency", getMetricValue(row.get(2)));
            queryMetrics.increase("minQueryLatency", getMetricValue(row.get(3)));
        }

        return queryMetrics;
    }

    public MetricsResponse getJobMetrics(String startTime, String endTime, String projectName, String cubeName) {
        PrepareSqlRequest sqlRequest = SqlCreationUtil.createPrepareSqlRequestOfTotalJobMetrics(startTime, endTime,
                projectName, cubeName);
        SQLResponse sqlResponse = queryService.doQueryWithCache(sqlRequest, false);

        MetricsResponse jobMetrics = new MetricsResponse();
        if (!sqlResponse.getIsException()) {
            List<String> row = sqlResponse.getResults().get(0);
            jobMetrics.increase("jobCount", getMetricValue(row.get(0)));
            jobMetrics.increase("avgJobBuildTime", getMetricValue(row.get(1)));
            jobMetrics.increase("maxJobBuildTime", getMetricValue(row.get(2)));
            jobMetrics.increase("minJobBuildTime", getMetricValue(row.get(3)));
            jobMetrics.increase("avgJobExpansionRate", getMetricValue(row.get(4)));
        }

        return jobMetrics;
    }

    public MetricsResponse getChartData(String category, String projectName, String cubeName, String startTime,
            String endTime, String dimension, String measure) {
        try {
            PrepareSqlRequest sqlRequest = SqlCreationUtil.createPrepareSqlRequestOfChartMetrics(category, projectName,
                    cubeName, startTime, endTime, dimension, measure);
            SQLResponse sqlResponse = queryService.doQueryWithCache(sqlRequest, false);

            return transformChartData(sqlResponse);
        } catch (Exception e) {
            throw new BadRequestException("Bad request due to " + e);
        }
    }

    private MetricsResponse transformChartData(SQLResponse sqlResponse) {
        if (!sqlResponse.getIsException()) {
            MetricsResponse metrics = new MetricsResponse();
            List<List<String>> results = sqlResponse.getResults();
            for (List<String> result : results) {
                String dimension = result.get(0);
                if (dimension != null && !dimension.isEmpty()) {
                    String metric = result.get(1);
                    metrics.increase(dimension, getMetricValue(metric));
                }
            }
            return metrics;
        }
        return null;
    }

    private Float getMetricValue(String value) {
        if (value == null || value.isEmpty()) {
            return 0f;
        } else {
            return Float.valueOf(value);
        }
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or hasPermission(#project, 'ADMINISTRATION')")
    public void checkAuthorization(ProjectInstance project) throws AccessDeniedException {
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    public void checkAuthorization() throws AccessDeniedException {
    }

}