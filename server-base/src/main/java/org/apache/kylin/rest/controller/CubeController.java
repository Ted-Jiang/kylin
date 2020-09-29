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

package org.apache.kylin.rest.controller;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;

import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.common.util.StringUtil;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.cuboid.CuboidScheduler;
import org.apache.kylin.cube.cuboid.TreeCuboidScheduler;
import org.apache.kylin.cube.cuboid.algorithm.OptimizationBenefit;
import org.apache.kylin.cube.cuboid.algorithm.RecommendResult;
import org.apache.kylin.cube.model.CubeBuildTypeEnum;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.CubeJoinedFlatTableDesc;
import org.apache.kylin.cube.model.HBaseColumnDesc;
import org.apache.kylin.cube.model.HBaseColumnFamilyDesc;
import org.apache.kylin.cube.model.RowKeyColDesc;
import org.apache.kylin.dimension.DimensionEncoding;
import org.apache.kylin.dimension.DimensionEncodingFactory;
import org.apache.kylin.engine.mr.common.CuboidStatsReaderUtil;
import org.apache.kylin.job.JobInstance;
import org.apache.kylin.job.JoinedFlatTable;
import org.apache.kylin.job.exception.JobException;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.IJoinedFlatTableDesc;
import org.apache.kylin.metadata.model.ISourceAware;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.SegmentRange.TSRange;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.kylin.rest.exception.BadRequestException;
import org.apache.kylin.rest.exception.ForbiddenException;
import org.apache.kylin.rest.exception.InternalErrorException;
import org.apache.kylin.rest.exception.NotFoundException;
import org.apache.kylin.rest.msg.Message;
import org.apache.kylin.rest.msg.MsgPicker;
import org.apache.kylin.rest.request.CubeRequest;
import org.apache.kylin.rest.request.CubeWithAllRequest;
import org.apache.kylin.rest.request.JobBuildRequest;
import org.apache.kylin.rest.request.JobBuildRequest2;
import org.apache.kylin.rest.request.JobOptimizeRequest;
import org.apache.kylin.rest.request.LookupSnapshotBuildRequest;
import org.apache.kylin.rest.response.CubeInstanceResponse;
import org.apache.kylin.rest.response.CubeOptimizationTrendResponse;
import org.apache.kylin.rest.response.CuboidRecommendResponse;
import org.apache.kylin.rest.response.CuboidTreeResponse;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.GeneralResponse;
import org.apache.kylin.rest.response.HBaseResponse;
import org.apache.kylin.rest.response.ResponseCode;
import org.apache.kylin.rest.service.CubeService;
import org.apache.kylin.rest.service.JobService;
import org.apache.kylin.rest.service.ModelService;
import org.apache.kylin.rest.service.ProjectService;
import org.apache.kylin.rest.service.QueryService;
import org.apache.kylin.rest.service.TableService;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.ValidateUtil;
import org.apache.kylin.shaded.com.google.common.collect.Lists;
import org.apache.kylin.shaded.com.google.common.collect.Maps;
import org.apache.kylin.shaded.com.google.common.collect.Sets;
import org.apache.kylin.source.kafka.util.KafkaClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;

/**
 * CubeController is defined as Restful API entrance for UI.
 */
@Controller
@RequestMapping(value = "/cubes")
public class CubeController extends BasicController {
    private static final Logger logger = LoggerFactory.getLogger(CubeController.class);

    @Autowired
    @Qualifier("cubeMgmtService")
    private CubeService cubeService;

    @Autowired
    @Qualifier("jobService")
    private JobService jobService;

    @Autowired
    @Qualifier("projectService")
    private ProjectService projectService;

    @Autowired
    @Qualifier("queryService")
    private QueryService queryService;

    @Autowired
    @Qualifier("modelMgmtService")
    private ModelService modelService;

    @Autowired
    @Qualifier("tableService")
    private TableService tableService;

    @Autowired
    @Qualifier("validateUtil")
    private ValidateUtil validateUtil;

    @Autowired
    private AclEvaluate aclEvaluate;

    @RequestMapping(value = "{cubeName}/validate", method = RequestMethod.GET, produces = { "application/json" })
    @ResponseBody
    public EnvelopeResponse<Boolean> validateCubeName(@PathVariable String cubeName) {
        return new EnvelopeResponse<>(ResponseCode.CODE_SUCCESS, cubeService.isCubeNameVaildate(cubeName), "");
    }

    @RequestMapping(value = "", method = { RequestMethod.GET }, produces = { "application/json" })
    @ResponseBody
    public List<CubeInstanceResponse> getCubes(@RequestParam(value = "cubeName", required = false) String cubeName,
            @RequestParam(value = "modelName", required = false) String modelName,
            @RequestParam(value = "projectName", required = false) String projectName,
            @RequestParam(value = "limit", required = false) Integer limit,
            @RequestParam(value = "offset", required = false) Integer offset) {
        List<CubeInstance> cubes = cubeService.listAllCubes(cubeName, projectName, modelName, false);

        List<CubeInstanceResponse> response = Lists.newArrayListWithExpectedSize(cubes.size());
        for (CubeInstance cube : cubes) {
            try {
                response.add(cubeService.createCubeInstanceResponse(cube));
            } catch (Exception e) {
                logger.error("Error creating cube instance response, skipping.", e);
            }
        }

        int climit = (null == limit) ? response.size() : limit;
        int coffset = (null == offset) ? 0 : offset;

        if (response.size() <= coffset) {
            return Collections.emptyList();
        }

        if ((response.size() - coffset) < climit) {
            return response.subList(coffset, response.size());
        }

        return response.subList(coffset, coffset + climit);
    }

    @RequestMapping(value = "validEncodings", method = { RequestMethod.GET }, produces = { "application/json" })
    @ResponseBody
    public Map<String, Integer> getValidEncodings() {
        Map<String, Integer> encodings;
        try {
            encodings = DimensionEncodingFactory.getValidEncodings();
        } catch (Exception e) {
            logger.error("Error when getting valid encodings", e);
            return Maps.newHashMap();
        }
        return encodings;
    }

    @RequestMapping(value = "/{cubeName}", method = { RequestMethod.GET }, produces = { "application/json" })
    @ResponseBody
    public CubeInstance getCube(@PathVariable String cubeName) {
        checkCubeExists(cubeName);
        return cubeService.getCubeManager().getCube(cubeName);
    }

    /**
     * Get SQL of a Cube
     *
     * @param cubeName Cube Name
     * @return
     * @throws IOException
     */
    @RequestMapping(value = "/{cubeName}/sql", method = { RequestMethod.GET }, produces = { "application/json" })
    @ResponseBody
    public GeneralResponse getSql(@PathVariable String cubeName) {
        checkCubeExists(cubeName);
        CubeInstance cube = cubeService.getCubeManager().getCube(cubeName);
        IJoinedFlatTableDesc flatTableDesc = new CubeJoinedFlatTableDesc(cube.getDescriptor(), true);
        String sql = JoinedFlatTable.generateSelectDataStatement(flatTableDesc);

        GeneralResponse response = new GeneralResponse();
        response.setProperty("sql", sql);

        return response;
    }

    /**
     * Get SQL of a Cube segment
     *
     * @param cubeName    Cube Name
     * @param segmentName Segment Name
     * @return
     * @throws IOException
     */
    @RequestMapping(value = "/{cubeName}/segs/{segmentName}/sql", method = { RequestMethod.GET }, produces = {
            "application/json" })
    @ResponseBody
    public GeneralResponse getSql(@PathVariable String cubeName, @PathVariable String segmentName) {

        checkCubeExists(cubeName);
        CubeInstance cube = cubeService.getCubeManager().getCube(cubeName);

        CubeSegment segment = cube.getSegment(segmentName, null);
        if (segment == null) {
            throw new NotFoundException("Cannot find segment " + segmentName);
        }

        IJoinedFlatTableDesc flatTableDesc = new CubeJoinedFlatTableDesc(segment, true);
        String sql = JoinedFlatTable.generateSelectDataStatement(flatTableDesc);

        GeneralResponse response = new GeneralResponse();
        response.setProperty("sql", sql);

        return response;
    }

    /**
     * Update cube notify list
     *
     * @param cubeName
     * @param notifyList
     * @throws IOException
     */
    @RequestMapping(value = "/{cubeName}/notify_list", method = { RequestMethod.PUT }, produces = {
            "application/json" })
    @ResponseBody
    public void updateNotifyList(@PathVariable String cubeName, @RequestBody List<String> notifyList) {
        checkCubeExists(cubeName);
        CubeInstance cube = cubeService.getCubeManager().getCube(cubeName);
        try {
            cubeService.updateCubeNotifyList(cube, notifyList);
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            throw new InternalErrorException(e.getLocalizedMessage(), e);
        }
    }

    /**
     * Update cube owner
     *
     * @param cubeName
     * @param owner
     * @throws IOException
     */
    @RequestMapping(value = "/{cubeName}/owner", method = { RequestMethod.PUT }, produces = {
        "application/json" })
    @ResponseBody
    public CubeInstance updateCubeOwner(@PathVariable String cubeName, @RequestBody String owner) {
        checkCubeExists(cubeName);
        try {
            validateUtil.checkIdentifiersExists(owner, true);
            CubeInstance cube = cubeService.getCubeManager().getCube(cubeName);
            return cubeService.updateCubeOwner(cube, owner);
        } catch (AccessDeniedException accessDeniedException) {
            throw new ForbiddenException("You don't have right to update this cube's owner.");
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            throw new InternalErrorException(e.getLocalizedMessage(), e);
        }
    }

    @RequestMapping(value = "/{cubeName}/cost", method = { RequestMethod.PUT }, produces = { "application/json" })
    @ResponseBody
    public CubeInstance updateCubeCost(@PathVariable String cubeName, @RequestParam(value = "cost") int cost) {
        checkCubeExists(cubeName);
        CubeInstance cube = cubeService.getCubeManager().getCube(cubeName);
        try {
            return cubeService.updateCubeCost(cube, cost);
        } catch (Exception e) {
            String message = "Failed to update cube cost: " + cubeName + " : " + cost;
            logger.error(message, e);
            throw new InternalErrorException(message + " Caused by: " + e.getMessage(), e);
        }
    }

    /**
     * Force rebuild a cube's lookup table snapshot
     *
     * @throws IOException
     */
    @RequestMapping(value = "/{cubeName}/segs/{segmentName}/refresh_lookup", method = {
            RequestMethod.PUT }, produces = { "application/json" })
    @ResponseBody
    public CubeInstance rebuildLookupSnapshot(@PathVariable String cubeName, @PathVariable String segmentName,
            @RequestParam(value = "lookupTable") String lookupTable) {
        try {
            final CubeManager cubeMgr = cubeService.getCubeManager();
            final CubeInstance cube = cubeMgr.getCube(cubeName);
            return cubeService.rebuildLookupSnapshot(cube, segmentName, lookupTable);
        } catch (IOException e) {
            logger.error(e.getLocalizedMessage(), e);
            throw new InternalErrorException(e.getLocalizedMessage(), e);
        }
    }

    /**
     * Force rebuild a cube's lookup table snapshot
     *
     * @throws IOException
     */
    @RequestMapping(value = "/{cubeName}/refresh_lookup", method = { RequestMethod.PUT }, produces = {
            "application/json" })
    @ResponseBody
    public JobInstance rebuildLookupSnapshot(@PathVariable String cubeName,
            @RequestBody LookupSnapshotBuildRequest request) {
        try {
            final CubeManager cubeMgr = cubeService.getCubeManager();
            final CubeInstance cube = cubeMgr.getCube(cubeName);
            String submitter = SecurityContextHolder.getContext().getAuthentication().getName();
            return jobService.submitLookupSnapshotJob(cube, request.getLookupTableName(), request.getSegmentIDs(),
                    submitter);
        } catch (IOException e) {
            logger.error(e.getLocalizedMessage(), e);
            throw new InternalErrorException(e.getLocalizedMessage(), e);
        }
    }

    /**
     * Delete a cube segment
     *
     * @throws IOException
     */
    @RequestMapping(value = "/{cubeName}/segs/{segmentName}", method = { RequestMethod.DELETE }, produces = {
            "application/json" })
    @ResponseBody
    public CubeInstance deleteSegment(@PathVariable String cubeName, @PathVariable String segmentName) {
        checkCubeExists(cubeName);
        CubeInstance cube = cubeService.getCubeManager().getCube(cubeName);

        CubeSegment segment = cube.getSegment(segmentName, null);
        if (segment == null) {
            throw new NotFoundException("Cannot find segment '" + segmentName + "'");
        }

        try {
            return cubeService.deleteSegment(cube, segmentName);
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            throw new InternalErrorException(e.getLocalizedMessage(), e);
        }
    }

    /**
     * Build/Rebuild a cube segment
     */
    @RequestMapping(value = "/{cubeName}/build", method = { RequestMethod.PUT }, produces = { "application/json" })
    @ResponseBody
    public JobInstance build(@PathVariable String cubeName, @RequestBody JobBuildRequest req) {
        return rebuild(cubeName, req);
    }

    /** Build/Rebuild a cube segment */

    /**
     * Build/Rebuild a cube segment
     */
    @RequestMapping(value = "/{cubeName}/rebuild", method = { RequestMethod.PUT }, produces = { "application/json" })
    @ResponseBody
    public JobInstance rebuild(@PathVariable String cubeName, @RequestBody JobBuildRequest req) {
        return buildInternal(cubeName, new TSRange(req.getStartTime(), req.getEndTime()), null, null, null,
                req.getBuildType(), req.isForce() || req.isForceMergeEmptySegment(), req.getPriorityOffset());
    }

    /**
     * Build/Rebuild a cube segment by source offset
     */
    @RequestMapping(value = "/{cubeName}/build2", method = { RequestMethod.PUT }, produces = { "application/json" })
    @ResponseBody
    public JobInstance build2(@PathVariable String cubeName, @RequestBody JobBuildRequest2 req) {
        try {
            Class<?> clazz = Class.forName("org.apache.kafka.clients.consumer.KafkaConsumer");
            if (clazz == null) {
                throw new ClassNotFoundException();
            }
        } catch (ClassNotFoundException e) {
            throw new InternalErrorException("Could not find Kafka dependency");
        }
        return rebuild2(cubeName, req);
    }

    /**
     * Build/Rebuild a cube segment by source offset
     */
    @RequestMapping(value = "/{cubeName}/rebuild2", method = { RequestMethod.PUT }, produces = { "application/json" })
    @ResponseBody
    public JobInstance rebuild2(@PathVariable String cubeName, @RequestBody JobBuildRequest2 req) {
        return buildInternal(cubeName, null, new SegmentRange(req.getSourceOffsetStart(), req.getSourceOffsetEnd()),
                req.getSourcePartitionOffsetStart(), req.getSourcePartitionOffsetEnd(), req.getBuildType(),
                req.isForce(), req.getPriorityOffset());
    }

    private JobInstance buildInternal(String cubeName, TSRange tsRange, SegmentRange segRange, //
            Map<Integer, Long> sourcePartitionOffsetStart, Map<Integer, Long> sourcePartitionOffsetEnd,
            String buildType, boolean force, Integer priorityOffset) {
        try {
            String submitter = SecurityContextHolder.getContext().getAuthentication().getName();
            CubeInstance cube = jobService.getCubeManager().getCube(cubeName);

            return jobService.submitJob(cube, tsRange, segRange, sourcePartitionOffsetStart, sourcePartitionOffsetEnd,
                    CubeBuildTypeEnum.valueOf(buildType), force, submitter, priorityOffset);
        } catch (Throwable e) {
            logger.error(e.getLocalizedMessage(), e);
            throw new InternalErrorException(e.getLocalizedMessage(), e);
        }
    }

    /**
     * Send a auto merge cube job
     *
     * @param cubeName Cube ID
     * @return JobInstance of merging cube
     */
    @RequestMapping(value = "/{cubeName}/automerge", method = { RequestMethod.PUT })
    @ResponseBody
    public JobInstance autoMerge(@PathVariable String cubeName) {
        try {
            checkCubeExists(cubeName);

            CubeInstance cube = jobService.getCubeManager().getCube(cubeName);
            aclEvaluate.checkProjectAdminPermission(cube.getProject());

            String jobID = cubeService.mergeCubeSegment(cubeName);
            if (jobID == null) {
                throw new BadRequestException(String.format(Locale.ROOT,
                        "Cube: %s merging is not supported or no segments to merge", cubeName));
            }
            return jobService.getJobInstance(jobID);
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            throw new InternalErrorException(e.getLocalizedMessage());
        }
    }

    /**
     * Send a optimize cube job
     *
     * @param cubeName Cube ID
     * @return JobInstance of CheckpointExecutable
     */
    @RequestMapping(value = "/{cubeName}/optimize", method = { RequestMethod.PUT })
    @ResponseBody
    public JobInstance optimize(@PathVariable String cubeName, @RequestBody JobOptimizeRequest jobOptimizeRequest) {
        try {
            String submitter = SecurityContextHolder.getContext().getAuthentication().getName();
            CubeInstance cube = jobService.getCubeManager().getCube(cubeName);

            checkCubeExists(cubeName);
            logger.info("cuboid recommend:" + jobOptimizeRequest.getCuboidsRecommend());
            return jobService.submitOptimizeJob(cube, jobOptimizeRequest.getCuboidsRecommend(), submitter).getFirst();
        } catch (BadRequestException e) {
            logger.error(e.getLocalizedMessage(), e);
            throw e;
        } catch (JobException e) {
            logger.error(e.getLocalizedMessage(), e);
            throw new BadRequestException(e.getLocalizedMessage());
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            throw new InternalErrorException(e.getLocalizedMessage(), e);
        }
    }

    /**
     * Send a optimize cube segment job
     *
     * @param cubeName  Cube ID
     * @param segmentID for segment to be optimized
     */
    @RequestMapping(value = "/{cubeName}/recover_segment_optimize/{segmentID}", method = { RequestMethod.PUT })
    @ResponseBody
    public JobInstance recoverSegmentOptimize(@PathVariable String cubeName, @PathVariable String segmentID) {
        try {
            String submitter = SecurityContextHolder.getContext().getAuthentication().getName();
            CubeInstance cube = jobService.getCubeManager().getCube(cubeName);

            CubeSegment segment = cube.getSegmentById(segmentID);
            if (segment == null) {
                throw new NotFoundException("Cannot find segment '" + segmentID + "'");
            }

            return jobService.submitRecoverSegmentOptimizeJob(segment, submitter);
        } catch (JobException e) {
            logger.error(e.getLocalizedMessage(), e);
            throw new BadRequestException(e.getLocalizedMessage());
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            throw new InternalErrorException(e.getLocalizedMessage(), e);
        }
    }

    @RequestMapping(value = "/{cubeName}/disable", method = { RequestMethod.PUT }, produces = { "application/json" })
    @ResponseBody
    public CubeInstance disableCube(@PathVariable String cubeName) {
        try {
            checkCubeExists(cubeName);
            CubeInstance cube = cubeService.getCubeManager().getCube(cubeName);

            return cubeService.disableCube(cube);
        } catch (Exception e) {
            String message = "Failed to disable cube: " + cubeName;
            logger.error(message, e);
            throw new InternalErrorException(message + " Caused by: " + e.getMessage(), e);
        }
    }

    @RequestMapping(value = "/{cubeName}/purge", method = { RequestMethod.PUT }, produces = { "application/json" })
    @ResponseBody
    public CubeInstance purgeCube(@PathVariable String cubeName) {
        try {
            checkCubeExists(cubeName);
            CubeInstance cube = cubeService.getCubeManager().getCube(cubeName);

            return cubeService.purgeCube(cube);
        } catch (Exception e) {
            String message = "Failed to purge cube: " + cubeName;
            logger.error(message, e);
            throw new InternalErrorException(message + " Caused by: " + e.getMessage(), e);
        }
    }

    @RequestMapping(value = "/{cubeName}/clone", method = { RequestMethod.PUT }, produces = { "application/json" })
    @ResponseBody
    public CubeInstance cloneCube(@PathVariable String cubeName, @RequestBody CubeRequest cubeRequest) {
        String newCubeName = cubeRequest.getCubeName();
        String projectName = cubeRequest.getProject();

        checkCubeExists(cubeName);
        CubeInstance cube = cubeService.getCubeManager().getCube(cubeName);
        if (cube.getStatus() == RealizationStatusEnum.DESCBROKEN) {
            throw new BadRequestException("Broken cube can't be cloned");
        }
        if (!ValidateUtil.isAlphanumericUnderscore(newCubeName)) {
            throw new BadRequestException("Invalid Cube name, only letters, numbers and underscore supported.");
        }

        ProjectInstance project = cubeService.getProjectManager().getProject(projectName);
        if (project == null) {
            throw new NotFoundException("Project " + projectName + " doesn't exist");
        }
        // KYLIN-1925, forbid cloning cross projects
        if (!project.getName().equals(cube.getProject())) {
            throw new BadRequestException("Cloning cubes across projects is not supported.");
        }

        CubeDesc cubeDesc = cube.getDescriptor();
        CubeDesc newCubeDesc = CubeDesc.getCopyOf(cubeDesc);

        newCubeDesc.setName(newCubeName);

        CubeInstance newCube;
        try {
            newCube = cubeService.createCubeAndDesc(project, newCubeDesc);

            //reload to avoid shallow clone
            cubeService.getCubeDescManager().reloadCubeDescLocal(newCubeName);
        } catch (IOException e) {
            throw new InternalErrorException("Failed to clone cube ", e);
        }

        return newCube;

    }

    @RequestMapping(value = "/{cubeName}/enable", method = { RequestMethod.PUT }, produces = { "application/json" })
    @ResponseBody
    public CubeInstance enableCube(@PathVariable String cubeName) {
        try {
            checkCubeExists(cubeName);
            CubeInstance cube = cubeService.getCubeManager().getCube(cubeName);
            cubeService.checkEnableCubeCondition(cube);

            return cubeService.enableCube(cube);
        } catch (Exception e) {
            String message = "Failed to enable cube: " + cubeName;
            logger.error(message, e);
            throw new InternalErrorException(message + " Caused by: " + e.getMessage(), e);
        }
    }

    @RequestMapping(value = "/{cubeName}", method = { RequestMethod.DELETE }, produces = { "application/json" })
    @ResponseBody
    public void deleteCube(@PathVariable String cubeName) {
        checkCubeExists(cubeName);
        CubeInstance cube = cubeService.getCubeManager().getCube(cubeName);

        //drop Cube
        try {
            cubeService.deleteCube(cube, true);
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            throw new InternalErrorException("Failed to delete cube. " + " Caused by: " + e.getMessage(), e);
        }

    }

    @RequestMapping(value = "/{cubeName}/withAll", method = { RequestMethod.DELETE }, produces = { "application/json" })
    @ResponseBody
    public GeneralResponse deleteCubeWithAll(@PathVariable String cubeName) {
        CubeInstance cube = getCube(cubeName);

        DataModelDesc model = cube.getModel();
        ProjectInstance project = cubeService.getProjectManager().getProjectOfModel(model.getName());
        Set<TableRef> tables = model.getAllTables();

        //drop Cube
        try {
            cubeService.deleteCube(cube, false);
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            throw new InternalErrorException("Failed to delete cube caused by: " + e.getMessage(), e);
        }

        GeneralResponse response = new GeneralResponse();
        StringBuilder warningMsg = new StringBuilder();

        //drop Model
        try {
            modelService.dropModel(model);
        } catch (Exception e) {
            logger.warn("", e);
            warningMsg.append(e);

            response.setProperty("Warning message:", warningMsg.toString());
            return response;
        }

        //unload tables
        Set<String> unLoadFail = com.google.common.collect.Sets.newHashSet();
        for (TableRef table : tables) {
            try {
                boolean ifUnloaded = tableService.unloadHiveTable(table.getTableIdentity(), project.getName());
                if (!ifUnloaded) {
                    unLoadFail.add(table.getTableIdentity());
                }
            } catch (Exception e) {
                logger.error("Failed to unload Hive Table", e);
                throw new InternalErrorException(e.getLocalizedMessage());
            }
        }
        if (!unLoadFail.isEmpty()) {
            warningMsg.append("Fail to unload tables: ").append(unLoadFail);
        }

        response.setProperty("Warning message:", warningMsg.toString());
        return response;
    }

    /**
     * Save cubeDesc with loading tables, creating model
     * @param cubeWithAllRequest
     * @return
     * @throws JsonProcessingException
     */
    @RequestMapping(value = "/withAll", method = { RequestMethod.POST }, produces = { "application/json" })
    @ResponseBody
    public CubeWithAllRequest saveCubeDescWithAll(@RequestBody CubeWithAllRequest cubeWithAllRequest) {
        if (cubeWithAllRequest == null) {
            logger.info("CubeWithAllRequest should not be null.");
            throw new BadRequestException("CubeWithAllRequest should not be null.");
        }

        Message msg = MsgPicker.getMsg();

        String projectName = (null == cubeWithAllRequest.getProject()) ? ProjectInstance.DEFAULT_PROJECT_NAME
                : cubeWithAllRequest.getProject();
        ProjectInstance project = cubeService.getProjectManager().getProject(projectName);
        if (project == null) {
            throw new BadRequestException(msg.getPROJECT_NOT_FOUND());
        }

        DataModelDesc model;
        try {
            model = JsonUtil.readValue(cubeWithAllRequest.getModelDescData(), DataModelDesc.class);
            model.setUuid(UUID.randomUUID().toString());
            model.setProjectName(projectName);
        } catch (Exception e) {
            throw new BadRequestException("Fail to deserialize modelDescData");
        }

        CubeDesc cubeDesc;
        try {
            cubeDesc = JsonUtil.readValue(cubeWithAllRequest.getCubeDescData(), CubeDesc.class);
            cubeDesc.setUuid(UUID.randomUUID().toString());
        } catch (Exception e) {
            throw new BadRequestException("Fail to deserialize cubeDescData");
        }

        String[] tableNames = StringUtil.splitAndTrim(cubeWithAllRequest.getTables(), ",");
        try {
            tableService.loadHiveTablesToProject(tableNames, project.getName());
        } catch (Exception e) {
            throw new BadRequestException(String.format(Locale.ROOT, "Fail to load tables %s due to %s",
                    cubeWithAllRequest.getTables(), e.getLocalizedMessage()));
        }

        try {
            DataModelDesc existingModel = modelService.getDataModelManager().getDataModelDesc(model.getName());
            if (existingModel != null) {
                cubeWithAllRequest
                        .appendMessage("model " + model.getName() + " already exist and will using existing one");
                model = existingModel;
            } else {
                modelService.createModelDesc(projectName, model);
            }
        } catch (Exception e) {
            throw new BadRequestException(String.format(Locale.ROOT, "Fail to create model %s due to %s",
                    model.getName(), e.getLocalizedMessage()));
        }

        try {
            cubeService.createCubeAndDesc(project, cubeDesc);
        } catch (Exception e) {
            throw new BadRequestException(String.format(Locale.ROOT, "Fail to create cube %s due to %s",
                    cubeDesc.getName(), e.getLocalizedMessage()));
        }

        cubeWithAllRequest.setModelUuid(model.getUuid());
        cubeWithAllRequest.setCubeUuid(cubeDesc.getUuid());
        return cubeWithAllRequest;
    }

    /**
     * save cubeDesc
     *
     * @return Table metadata array
     * @throws IOException
     */
    @RequestMapping(value = "", method = { RequestMethod.POST }, produces = { "application/json" })
    @ResponseBody
    public CubeRequest saveCubeDesc(@RequestBody CubeRequest cubeRequest) {

        CubeDesc desc = deserializeCubeDesc(cubeRequest);

        if (desc == null) {
            cubeRequest.setMessage("CubeDesc is null.");
            return cubeRequest;
        }
        String name = desc.getName();
        if (StringUtils.isEmpty(name)) {
            logger.info("Cube name should not be empty.");
            throw new BadRequestException("Cube name should not be empty.");
        }
        if (!ValidateUtil.isAlphanumericUnderscore(name)) {
            throw new BadRequestException("Invalid Cube name, only letters, numbers and underscore supported.");
        }

        validateColumnFamily(desc);

        try {
            desc.setUuid(RandomUtil.randomUUID().toString());
            String projectName = (null == cubeRequest.getProject()) ? ProjectInstance.DEFAULT_PROJECT_NAME
                    : cubeRequest.getProject();
            ProjectInstance project = cubeService.getProjectManager().getProject(projectName);
            if (project == null) {
                throw new NotFoundException("Project " + projectName + " doesn't exist");
            }
            cubeService.createCubeAndDesc(project, desc);
        } catch (Exception e) {
            logger.error("Failed to deal with the request.", e);
            throw new InternalErrorException(e.getLocalizedMessage(), e);
        }

        cubeRequest.setUuid(desc.getUuid());
        cubeRequest.setSuccessful(true);
        return cubeRequest;
    }

    //column family metrics may not match the real metrics when editing cube by json,see MTHDP-5091
    private void validateColumnFamily(CubeDesc cubeDesc) {
        Set<String> columnFamilyMetricsSet = Sets.newHashSet();
        for (HBaseColumnFamilyDesc hBaseColumnFamilyDesc : cubeDesc.getHbaseMapping().getColumnFamily()) {
            for (HBaseColumnDesc hBaseColumnDesc : hBaseColumnFamilyDesc.getColumns()) {
                for (String columnName : hBaseColumnDesc.getMeasureRefs()) {
                    columnFamilyMetricsSet.add(columnName);
                }
            }
        }
        for (MeasureDesc measureDesc : cubeDesc.getMeasures()) {
            if (!columnFamilyMetricsSet.contains(measureDesc.getName())) {
                throw new BadRequestException("column family lack measure:" + measureDesc.getName());
            }
        }
        if (cubeDesc.getMeasures().size() != columnFamilyMetricsSet.size()) {
            throw new BadRequestException(
                    "the number of input measure and the number of measure defined in cubedesc are not consistent");
        }

        for (RowKeyColDesc rowKeyColDesc : cubeDesc.getRowkey().getRowKeyColumns()) {
            Object[] encodingConf = DimensionEncoding.parseEncodingConf(rowKeyColDesc.getEncoding());
            String encodingName = (String) encodingConf[0];
            String[] encodingArgs = (String[]) encodingConf[1];

            if (!DimensionEncodingFactory.isValidEncoding(encodingName, encodingArgs,
                    rowKeyColDesc.getEncodingVersion())) {
                throw new BadRequestException("Illegal row key column desc: " + rowKeyColDesc);
            }
        }
    }

    /**
     * update CubDesc
     *
     * @return Table metadata array
     * @throws JsonProcessingException
     * @throws IOException
     */
    @RequestMapping(value = "", method = { RequestMethod.PUT }, produces = { "application/json" })
    @ResponseBody
    public CubeRequest updateCubeDesc(@RequestBody CubeRequest cubeRequest) throws JsonProcessingException {

        CubeDesc desc = deserializeCubeDesc(cubeRequest);
        if (desc == null) {
            return cubeRequest;
        }

        String projectName = (null == cubeRequest.getProject()) ? ProjectInstance.DEFAULT_PROJECT_NAME
                : cubeRequest.getProject();
        try {
            CubeInstance cube = cubeService.getCubeManager().getCube(cubeRequest.getCubeName());

            if (cube == null) {
                String error = "The cube named " + cubeRequest.getCubeName() + " does not exist ";
                updateRequest(cubeRequest, false, error);
                return cubeRequest;
            }

            validateColumnFamily(desc);

            //cube renaming is not allowed
            if (!cube.getDescriptor().getName().equalsIgnoreCase(desc.getName())) {
                String error = "Cube Desc renaming is not allowed: desc.getName(): " + desc.getName()
                        + ", cubeRequest.getCubeName(): " + cubeRequest.getCubeName();
                updateRequest(cubeRequest, false, error);
                return cubeRequest;
            }

            if (cube.getSegments().size() != 0 && !cube.getDescriptor().consistentWith(desc)) {
                String error = "CubeDesc " + desc.getName()
                        + " is inconsistent with existing. Try purge that cube first or avoid updating key cube desc fields.";
                updateRequest(cubeRequest, false, error);
                return cubeRequest;
            }

            desc = cubeService.updateCubeAndDesc(cube, desc, projectName, true);

        } catch (AccessDeniedException accessDeniedException) {
            throw new ForbiddenException("You don't have right to update this cube.");
        } catch (Exception e) {
            logger.error("Failed to deal with the request:" + e.getLocalizedMessage(), e);
            throw new InternalErrorException("Failed to deal with the request: " + e.getLocalizedMessage(), e);
        }

        if (desc.isBroken()) {
            updateRequest(cubeRequest, false, desc.getErrorsAsString());
            return cubeRequest;
        }

        String descData = JsonUtil.writeValueAsIndentString(desc);
        cubeRequest.setCubeDescData(descData);
        cubeRequest.setSuccessful(true);
        return cubeRequest;
    }

    /**
     * get Hbase Info
     *
     * @return true
     * @throws IOException
     */
    @RequestMapping(value = "/{cubeName}/hbase", method = { RequestMethod.GET }, produces = { "application/json" })
    @ResponseBody
    public List<HBaseResponse> getHBaseInfo(@PathVariable String cubeName) {
        List<HBaseResponse> hbase = new ArrayList<HBaseResponse>();

        CubeInstance cube = cubeService.getCubeManager().getCube(cubeName);
        if (null == cube) {
            throw new InternalErrorException("Cannot find cube " + cubeName);
        }

        List<CubeSegment> segments = cube.getSegments();

        for (CubeSegment segment : segments) {
            String tableName = segment.getStorageLocationIdentifier();
            HBaseResponse hr = null;

            // Get info of given table.
            try {
                hr = cubeService.getHTableInfo(cubeName, tableName);
            } catch (IOException e) {
                logger.error("Failed to calcuate size of HTable \"" + tableName + "\".", e);
            }

            if (null == hr) {
                logger.info("Failed to calcuate size of HTable \"" + tableName + "\".");
                hr = new HBaseResponse();
            }

            hr.setTableName(tableName);
            hr.setDateRangeStart(segment.getTSRange().start.v);
            hr.setDateRangeEnd(segment.getTSRange().end.v);
            hr.setSegmentName(segment.getName());
            hr.setSegmentStatus(segment.getStatus().toString());
            hr.setSourceCount(segment.getInputRecords());
            if (segment.isOffsetCube()) {
                hr.setSourceOffsetStart((Long) segment.getSegRange().start.v);
                hr.setSourceOffsetEnd((Long) segment.getSegRange().end.v);
            }
            hbase.add(hr);
        }

        return hbase;
    }

    /**
     * get cube segment holes
     *
     * @return a list of CubeSegment, each representing a hole
     * @throws IOException
     */
    @RequestMapping(value = "/{cubeName}/holes", method = { RequestMethod.GET }, produces = { "application/json" })
    @ResponseBody
    public List<CubeSegment> getHoles(@PathVariable String cubeName) {
        checkCubeExists(cubeName);
        return cubeService.getCubeManager().calculateHoles(cubeName);
    }

    /**
     * fill cube segment holes
     *
     * @return a list of JobInstances to fill the holes
     * @throws IOException
     */
    @RequestMapping(value = "/{cubeName}/holes", method = { RequestMethod.PUT }, produces = { "application/json" })
    @ResponseBody
    public List<JobInstance> fillHoles(@PathVariable String cubeName) {
        checkCubeExists(cubeName);
        List<JobInstance> jobs = Lists.newArrayList();
        List<CubeSegment> holes = cubeService.getCubeManager().calculateHoles(cubeName);

        if (holes.size() == 0) {
            logger.info("No hole detected for cube '" + cubeName + "'");
            return jobs;
        }

        for (CubeSegment hole : holes) {
            if (hole.isOffsetCube()) {
                JobBuildRequest2 request = new JobBuildRequest2();
                request.setBuildType(CubeBuildTypeEnum.BUILD.toString());
                request.setSourceOffsetStart((Long) hole.getSegRange().start.v);
                request.setSourceOffsetEnd((Long) hole.getSegRange().end.v);
                request.setSourcePartitionOffsetStart(hole.getSourcePartitionOffsetStart());
                request.setSourcePartitionOffsetEnd(hole.getSourcePartitionOffsetEnd());
                try {
                    JobInstance job = build2(cubeName, request);
                    jobs.add(job);
                } catch (Exception e) {
                    // it may exceed the max allowed job number
                    logger.info("Error to submit job for hole '" + hole.toString() + "', skip it now.", e);
                    continue;
                }
            } else {
                JobBuildRequest request = new JobBuildRequest();
                request.setBuildType(CubeBuildTypeEnum.BUILD.toString());
                request.setStartTime(hole.getTSRange().start.v);
                request.setEndTime(hole.getTSRange().end.v);

                try {
                    JobInstance job = build(cubeName, request);
                    jobs.add(job);
                } catch (Exception e) {
                    // it may exceed the max allowed job number
                    logger.info("Error to submit job for hole '" + hole.toString() + "', skip it now.", e);
                    continue;
                }
            }
        }

        return jobs;

    }

    @RequestMapping(value = "/{cubeName}/cuboids/export", method = RequestMethod.GET)
    @ResponseBody
    public void cuboidsExport(@PathVariable String cubeName, @RequestParam(value = "top") Integer top,
            HttpServletResponse response) throws IOException {
        checkCubeExists(cubeName);
        CubeInstance cube = cubeService.getCubeManager().getCube(cubeName);

        Message msg = MsgPicker.getMsg();
        String errorMsg = String.format(Locale.ROOT, msg.getFAIL_RECOMMEND_CUBOID(), cubeName);

        RecommendResult recommRet = getRecommendCuboidList(cube);
        if (recommRet == null) {
            logger.warn(errorMsg);
            throw new InternalErrorException(errorMsg);
        }

        Map<Long, Long> cuboidList = recommRet.getRecommendCuboids();
        if (cuboidList == null || cuboidList.isEmpty()) {
            logger.warn(errorMsg);
            throw new InternalErrorException(errorMsg);
        }
        if (cuboidList.size() < top) {
            logger.info("Only recommend " + cuboidList.size() + " cuboids less than topn " + top);
        }
        Iterator<Long> cuboidIterator = cuboidList.keySet().iterator();
        RowKeyColDesc[] rowKeyColDescList = cube.getDescriptor().getRowkey().getRowKeyColumns();

        List<Set<String>> dimensionSetList = Lists.newLinkedList();
        while (top-- > 0 && cuboidIterator.hasNext()) {
            Set<String> dimensionSet = Sets.newHashSet();
            dimensionSetList.add(dimensionSet);
            long cuboid = cuboidIterator.next();
            for (int i = 0; i < rowKeyColDescList.length; i++) {
                if ((cuboid & (1L << rowKeyColDescList[i].getBitIndex())) > 0) {
                    dimensionSet.add(rowKeyColDescList[i].getColumn());
                }
            }
        }

        response.setContentType("text/json;charset=utf-8");
        response.setHeader("Content-Disposition",
                "attachment; filename=\"" + ValidateUtil.convertStringToBeAlphanumericUnderscore(cubeName) + ".json\"");
        try (PrintWriter writer = response.getWriter()) {
            writer.write(JsonUtil.writeValueAsString(dimensionSetList));
        } catch (IOException e) {
            logger.error("", e);
            throw new InternalErrorException("Failed to write: " + e.getLocalizedMessage(), e);
        }
    }

    @RequestMapping(value = "/{cubeName}/cuboids/current", method = RequestMethod.GET)
    @ResponseBody
    public CuboidTreeResponse getCurrentCuboids(@PathVariable String cubeName) {
        checkCubeExists(cubeName);
        CubeInstance cube = cubeService.getCubeManager().getCube(cubeName);

        // The cuboid tree displayed should be consistent with the current one
        CuboidScheduler cuboidScheduler = cube.getCuboidScheduler();
        Set<Long> currentCuboidSet = cuboidScheduler.getAllCuboidIds();
        Map<Long, Long> cuboidStatsMap = CuboidStatsReaderUtil.readCuboidStatsFromCube(currentCuboidSet, cube);

        Map<Long, Long> hitFrequencyMap = null;
        Map<Long, Long> queryMatchMap = null;
        try {
            hitFrequencyMap = getTargetCuboidHitFrequency(cubeName);
            queryMatchMap = cubeService.getCuboidQueryMatchCount(cubeName);
        } catch (Exception e) {
            logger.warn("Fail to query on system cube due to " + e);
        }

        return cubeService.getCuboidTreeResponse(cuboidScheduler, cuboidStatsMap, hitFrequencyMap, queryMatchMap,
                currentCuboidSet);
    }

    @RequestMapping(value = "/{cubeName}/cuboids/update", method = RequestMethod.PUT)
    @ResponseBody
    public CubeInstance updateCuboidBytes(@PathVariable String cubeName) throws IOException {
        checkCubeExists(cubeName);
        CubeInstance cube = cubeService.getCubeManager().getCube(cubeName);

        return cubeService.updateCuboidBytes(cube);
    }

    @RequestMapping(value = "/{cubeName}/cuboids/recommend", method = RequestMethod.GET)
    @ResponseBody
    public CuboidRecommendResponse getRecommendCuboids(@PathVariable String cubeName) throws IOException {
        checkCubeExists(cubeName);
        CubeInstance cube = cubeService.getCubeManager().getCube(cubeName);

        Message msg = MsgPicker.getMsg();
        String errorMsg = String.format(Locale.ROOT, msg.getFAIL_RECOMMEND_CUBOID(), cubeName);

        RecommendResult recommRet = getRecommendCuboidList(cube);
        if (recommRet == null) {
            logger.warn(errorMsg);
            return new CuboidRecommendResponse(new CuboidTreeResponse(), OptimizationBenefit.ZERO);
        }
        Map<Long, Long> recommendCuboidStatsMap = recommRet.getRecommendCuboids();
        if (recommendCuboidStatsMap == null || recommendCuboidStatsMap.isEmpty()) {
            logger.warn(errorMsg);
            return new CuboidRecommendResponse(new CuboidTreeResponse(), OptimizationBenefit.ZERO);
        }

        //update cube score
        cubeService.getCubeManager().saveCubeOptimizationBenefit(cube, recommRet.getOptimizationBenefit());

        CuboidScheduler cuboidScheduler = new TreeCuboidScheduler(cube.getDescriptor(),
                Lists.newArrayList(recommendCuboidStatsMap.keySet()),
                new TreeCuboidScheduler.CuboidCostComparator(recommendCuboidStatsMap));

        // Get cuboid target info for displaying heat map of cuboid hit
        Map<Long, Long> displayHitFrequencyMap = getTargetCuboidHitFrequency(cubeName);
        // Get exactly matched cuboid query count
        Map<Long, Long> queryMatchMap = cubeService.getCuboidQueryMatchCount(cubeName);

        Set<Long> currentCuboidSet = cube.getCuboidScheduler().getAllCuboidIds();

        CuboidTreeResponse cuboidTree = cubeService.getCuboidTreeResponse(cuboidScheduler, recommendCuboidStatsMap,
                displayHitFrequencyMap, queryMatchMap, currentCuboidSet);
        return new CuboidRecommendResponse(cuboidTree, recommRet.getOptimizationBenefit());
    }

    @RequestMapping(value = "/{cubeName}/optimization/trend", method = RequestMethod.GET)
    @ResponseBody
    public CubeOptimizationTrendResponse getOptimizationTrend(@PathVariable String cubeName) throws IOException {
        CubeInstance cube = getCube(cubeName);

        long endTimestamp = System.currentTimeMillis();
        TreeMap<String, Double> trendOfQueryLatency = cubeService.getQueryLatencyTrendForOptimization(cube,
                endTimestamp);
        TreeMap<String, Double> trendOfStorageUsage = cubeService.getStorageUsageTrendForOptimization(cube,
                endTimestamp);

        TreeSet<String> timeSequenece = new TreeSet<>();
        timeSequenece.addAll(trendOfQueryLatency.keySet());
        timeSequenece.addAll(trendOfStorageUsage.keySet());

        List<String> timeSequeneceList = new ArrayList<>(timeSequenece.size());
        List<Double> trendOfQueryLatencyList = new ArrayList<>(timeSequenece.size());
        List<Double> trendOfStorageUsageList = new ArrayList<>(timeSequenece.size());
        Double prevQueryLatency = trendOfQueryLatency.firstEntry().getValue();
        Double prevStorageUsage = trendOfStorageUsage.firstEntry().getValue();
        for (String timeKey : timeSequenece) {
            timeSequeneceList.add(timeKey);

            // query latency sequences
            Double queryLatency = trendOfQueryLatency.get(timeKey);
            if (queryLatency == null) {
                queryLatency = prevQueryLatency;
            } else {
                prevQueryLatency = queryLatency;
            }
            if (queryLatency != null) {
                trendOfQueryLatencyList.add(queryLatency);
            }

            // storage usage sequences
            Double storageUsage = trendOfStorageUsage.get(timeKey);
            if (storageUsage == null) {
                storageUsage = prevStorageUsage;
            } else {
                prevStorageUsage = storageUsage;
            }
            if (storageUsage != null) {
                trendOfStorageUsageList.add(storageUsage);
            }

        }
        return new CubeOptimizationTrendResponse(timeSequeneceList, trendOfQueryLatencyList, trendOfStorageUsageList);
    }

    private RecommendResult getRecommendCuboidList(CubeInstance cube) throws IOException {
        // Get cuboid source info
        Map<Long, Long> optimizeHitFrequencyMap = getSourceCuboidHitFrequency(cube.getName());
        Map<Long, Map<Long, Pair<Long, Long>>> rollingUpCountSourceMap = cubeService
                .getCuboidRollingUpStats(cube.getName());
        return cubeService.getRecommendCuboidStatistics(cube, optimizeHitFrequencyMap, rollingUpCountSourceMap);
    }

    private Map<Long, Long> getSourceCuboidHitFrequency(String cubeName) {
        return cubeService.getCuboidHitFrequency(cubeName, true);
    }

    private Map<Long, Long> getTargetCuboidHitFrequency(String cubeName) {
        return cubeService.getCuboidHitFrequency(cubeName, false);
    }

    /**
     * Initiate the very beginning of a streaming cube. Will seek the latest offests of each partition from streaming
     * source (kafka) and record in the cube descriptor; In the first build job, it will use these offests as the start point.
     *
     * @param cubeName
     * @return
     */
    @RequestMapping(value = "/{cubeName}/init_start_offsets", method = { RequestMethod.PUT }, produces = {
            "application/json" })
    @ResponseBody
    public GeneralResponse initStartOffsets(@PathVariable String cubeName) {
        checkCubeExists(cubeName);
        CubeInstance cubeInstance = cubeService.getCubeManager().getCube(cubeName);
        if (cubeInstance.getSourceType() != ISourceAware.ID_STREAMING) {
            String msg = "Cube '" + cubeName + "' is not a Streaming Cube.";
            throw new IllegalArgumentException(msg);
        }

        final GeneralResponse response = new GeneralResponse();
        try {
            final Map<Integer, Long> startOffsets = KafkaClient.getLatestOffsets(cubeInstance);
            CubeDesc desc = cubeInstance.getDescriptor();
            desc.setPartitionOffsetStart(startOffsets);
            cubeService.getCubeDescManager().updateCubeDesc(desc);
            response.setProperty("result", "success");
            response.setProperty("offsets", startOffsets.toString());
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }

        return response;
    }

    private CubeDesc deserializeCubeDesc(CubeRequest cubeRequest) {
        CubeDesc desc = null;
        try {
            logger.debug("Saving cube " + cubeRequest.getCubeDescData());
            desc = JsonUtil.readValue(cubeRequest.getCubeDescData(), CubeDesc.class);
        } catch (JsonParseException e) {
            logger.error("The cube definition is not valid.", e);
            updateRequest(cubeRequest, false, e.getMessage());
        } catch (JsonMappingException e) {
            logger.error("The cube definition is not valid.", e);
            updateRequest(cubeRequest, false, e.getMessage());
        } catch (IOException e) {
            logger.error("Failed to deal with the request.", e);
            throw new InternalErrorException("Failed to deal with the request:" + e.getMessage(), e);
        }
        return desc;
    }

    private void updateRequest(CubeRequest request, boolean success, String message) {
        request.setCubeDescData("");
        request.setSuccessful(success);
        request.setMessage(message);
    }

    private void checkCubeExists(String cubeName) {
        CubeInstance cubeInstance = cubeService.getCubeManager().getCube(cubeName);
        if (cubeInstance == null) {
            Message msg = MsgPicker.getMsg();
            throw new NotFoundException(String.format(Locale.ROOT, msg.getCUBE_NOT_FOUND(), cubeName));
        }
    }

    public void setCubeService(CubeService cubeService) {
        this.cubeService = cubeService;
    }

    public void setJobService(JobService jobService) {
        this.jobService = jobService;
    }

    public void setValidateUtil(ValidateUtil validateUtil) {
        this.validateUtil = validateUtil;
    }
}
