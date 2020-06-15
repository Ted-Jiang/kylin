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

package org.apache.kylin.engine.mr.steps;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.kv.RowKeyDecoder;
import org.apache.kylin.cube.model.RowKeyDesc;
import org.apache.kylin.engine.mr.KylinMapper;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.measure.BufferedMeasureCodec;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExportBaseCuboidMapper extends KylinMapper<Text, Text, NullWritable, Text> {

    private static final Logger logger = LoggerFactory.getLogger(ExportBaseCuboidMapper.class);

    private MultipleOutputs mos;
    private String delimiter;

    private RowKeyDecoder rowKeyDecoder;
    private long baseCuboidId;

    private TblColRef partCol;
    private int partitionColumnIndex = -1;

    private BufferedMeasureCodec codec;
    private Object[] measureResults;

    private int count = 0;

    @Override
    protected void doSetup(Context context) throws IOException {
        super.bindCurrentConfiguration(context.getConfiguration());
        mos = new MultipleOutputs(context);

        String cubeName = context.getConfiguration().get(BatchConstants.CFG_CUBE_NAME);
        String segmentID = context.getConfiguration().get(BatchConstants.CFG_CUBE_SEGMENT_ID);
        delimiter = context.getConfiguration().get(BatchConstants.CFG_DATA_EXPORT_DELIMITER);

        KylinConfig config = AbstractHadoopJob.loadKylinPropsAndMetadata();

        CubeManager cubeManager = CubeManager.getInstance(config);
        CubeInstance cube = cubeManager.getCube(cubeName);
        CubeSegment cubeSeg = cube.getSegmentById(segmentID);

        baseCuboidId = cube.getCuboidScheduler().getBaseCuboidId();

        rowKeyDecoder = new RowKeyDecoder(cubeSeg);
        partCol = cube.getModel().getPartitionDesc().getPartitionDateColumnRef();
        RowKeyDesc rowKeyDesc = cube.getDescriptor().getRowkey();
        partitionColumnIndex = rowKeyDesc.getRowKeyColumns().length - rowKeyDesc.getColumnBitIndex(partCol) - 1;

        List<MeasureDesc> measuresDescs = cube.getDescriptor().getMeasures();
        codec = new BufferedMeasureCodec(measuresDescs);
        measureResults = new Object[measuresDescs.size()];
    }

    @Override
    public void doMap(Text key, Text value, Context context) throws IOException, InterruptedException {
        long cuboidID = rowKeyDecoder.decode(key.getBytes());
        if (cuboidID != baseCuboidId) {
            return; // Skip data from cuboids which are not the base cuboid
        }

        List<String> keys = rowKeyDecoder.getValues();
        String baseOutputPath = partitionColumnIndex < 0 ? "NoPartition"
                : partCol.getName() + "=" + keys.remove(partitionColumnIndex);

        codec.decode(ByteBuffer.wrap(value.getBytes(), 0, value.getLength()), measureResults);

        StringBuilder sb = new StringBuilder();
        sb.append(StringUtils.join(keys, delimiter));
        if (measureResults.length > 0) {
            sb.append(delimiter);
            sb.append(StringUtils.join(measureResults, delimiter));
        }

        if (count++ % 10000 == 0) {
            logger.info("base cuboid value: " + sb.toString());
        }
        mos.write(NullWritable.get(), sb.toString(), generateFileName(baseOutputPath));
    }

    @Override
    public void doCleanup(Context context) throws IOException, InterruptedException {
        mos.close();
    }

    private String generateFileName(String subDir) {
        return subDir + "/part";
    }
}
