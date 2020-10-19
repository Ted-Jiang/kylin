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

package org.apache.kylin.measure.topn;

import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.dimension.DimensionEncoding;
import org.apache.kylin.measure.MeasureAggregator;
import org.apache.kylin.measure.MeasureIngester;
import org.apache.kylin.measure.MeasureType;
import org.apache.kylin.measure.MeasureTypeFactory;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.datatype.DataTypeSerializer;
import org.apache.kylin.metadata.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Iterator;

public class BiTopNMeasureType extends TopNMeasureTypeBase<ByteArray, BiTopNCounter<ByteArray>> {

    private static final Logger logger = LoggerFactory.getLogger(BiTopNMeasureType.class);

    public static final String DATATYPE_TOPN = "bi_topn";

    public static class Factory extends MeasureTypeFactory<BiTopNCounter<ByteArray>> {

        @Override
        public MeasureType<BiTopNCounter<ByteArray>> createMeasureType(String funcName, DataType dataType) {
            return new BiTopNMeasureType(dataType);
        }

        @Override
        public String getAggrFunctionName() {
            return FUNC_TOP_N;
        }

        @Override
        public String getAggrDataTypeName() {
            return DATATYPE_TOPN;
        }

        @Override
        public Class<? extends DataTypeSerializer<BiTopNCounter<ByteArray>>> getAggrDataTypeSerializer() {
            return BiTopNCounterSerializer.class;
        }
    }

    // ============================================================================

    public BiTopNMeasureType(DataType dataType) {
        super(dataType);
    }

    @Override
    protected void validateDataType(String dataType) {
        if (!DATATYPE_TOPN.equals(dataType))
            throw new IllegalArgumentException();
    }

    @Override
    public MeasureIngester<BiTopNCounter<ByteArray>> newIngester() {
        return new TopNMeasureIngester<ByteArray, BiTopNCounter<ByteArray>>() {

            protected BiTopNCounter<ByteArray> valueOf(String[] values) {
                // Construct key
                final ByteArray key = new ByteArray(keyLength);
                int offset = 0;
                for (int i = 0; i < dimensionEncodings.length; i++) {
                    if (values[i + 1] == null) {
                        Arrays.fill(key.array(), offset, offset + dimensionEncodings[i].getLengthOfEncoding(),
                                DimensionEncoding.NULL);
                    } else {
                        dimensionEncodings[i].encode(values[i + 1], key.array(), offset);
                    }
                    offset += dimensionEncodings[i].getLengthOfEncoding();
                }
                // Construct value
                double counter = values[0] == null ? 0 : Double.parseDouble(values[0]);

                BiTopNCounter<ByteArray> topNCounter = new BiTopNCounter<ByteArray>(
                        dataType.getPrecision() * BiTopNCounter.EXTRA_SPACE_RATE);
                topNCounter.offer(key, counter);
                return topNCounter;
            }

            protected BiTopNCounter<ByteArray> reEncodeDictionary(BiTopNCounter<ByteArray> value) {
                BiTopNCounter<ByteArray> topNCounter = value;

                int topNSize = topNCounter.size();
                byte[] newIdBuf = new byte[topNSize * newKeyLength];

                int bufOffset = 0;
                for (Counter<ByteArray> c : topNCounter) {
                    int offset = c.getItem().offset();
                    int innerBuffOffset = 0;
                    for (int i = 0; i < dimensionEncodings.length; i++) {
                        String dimValue = dimensionEncodings[i].decode(c.getItem().array(), offset,
                                dimensionEncodings[i].getLengthOfEncoding());
                        newDimensionEncodings[i].encode(dimValue, newIdBuf, bufOffset + innerBuffOffset);
                        innerBuffOffset += newDimensionEncodings[i].getLengthOfEncoding();
                        offset += dimensionEncodings[i].getLengthOfEncoding();
                    }

                    c.getItem().reset(newIdBuf, bufOffset, newKeyLength);
                    bufOffset += newKeyLength;
                }
                return topNCounter;
            }
        };
    }

    @Override
    protected IAdvMeasureFiller getTopNMeasureFiller(DimensionEncoding[] dimensionEncodings, int[] literalTupleIdx, int numericTupleIdx) {
        return new IAdvMeasureFiller() {
            private BiTopNCounter<ByteArray> topNCounter;
            private Iterator<Counter<ByteArray>> topNCounterIterator;
            private int expectRow = 0;

            @SuppressWarnings("unchecked")
            @Override
            public void reload(Object measureValue) {
                this.topNCounter = (BiTopNCounter<ByteArray>) measureValue;
                this.topNCounterIterator = topNCounter.iterator();
                this.expectRow = 0;
            }

            @Override
            public int getNumOfRows() {
                return topNCounter.size();
            }

            @Override
            public void fillTuple(Tuple tuple, int row) {
                if (expectRow++ != row)
                    throw new IllegalStateException();

                Counter<ByteArray> counter = topNCounterIterator.next();
                int offset = counter.getItem().offset();
                for (int i = 0; i < dimensionEncodings.length; i++) {
                    String colValue = dimensionEncodings[i].decode(counter.getItem().array(), offset,
                            dimensionEncodings[i].getLengthOfEncoding());
                    tuple.setDimensionValue(literalTupleIdx[i], colValue);
                    offset += dimensionEncodings[i].getLengthOfEncoding();
                }
                tuple.setMeasureValue(numericTupleIdx, counter.getCount());
                if (expectRow < 5) {
                    logger.debug("Fill tuple measure value at {} with {}", numericTupleIdx, counter.getCount());
                }
            }
        };
    }

    @Override
    public MeasureAggregator<BiTopNCounter<ByteArray>> newAggregator() {
        return new BiTopNAggregator();
    }
}
