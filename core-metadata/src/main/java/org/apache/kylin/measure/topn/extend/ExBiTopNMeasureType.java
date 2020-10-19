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

package org.apache.kylin.measure.topn.extend;

import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.dimension.DimensionEncoding;
import org.apache.kylin.measure.MeasureAggregator;
import org.apache.kylin.measure.MeasureIngester;
import org.apache.kylin.measure.MeasureType;
import org.apache.kylin.measure.MeasureTypeFactory;
import org.apache.kylin.measure.topn.Counter;
import org.apache.kylin.measure.topn.TopNMeasureTypeBase;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.datatype.DataTypeSerializer;
import org.apache.kylin.metadata.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Iterator;

public class ExBiTopNMeasureType extends TopNMeasureTypeBase<ExItem<ByteArray>, ExBiTopNCounter<ByteArray>> {

    private static final Logger logger = LoggerFactory.getLogger(ExBiTopNMeasureType.class);

    public static final String DATATYPE_TOPN = "ex_bi_topn";

    public static class Factory extends MeasureTypeFactory<ExBiTopNCounter<ByteArray>> {

        @Override
        public MeasureType<ExBiTopNCounter<ByteArray>> createMeasureType(String funcName, DataType dataType) {
            return new ExBiTopNMeasureType(dataType);
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
        public Class<? extends DataTypeSerializer<ExBiTopNCounter<ByteArray>>> getAggrDataTypeSerializer() {
            return ExBiTopNCounterSerializer.class;
        }
    }

    // ============================================================================

    public ExBiTopNMeasureType(DataType dataType) {
        super(dataType);
    }

    @Override
    protected void validateDataType(String dataType) {
        if (!DATATYPE_TOPN.equals(dataType))
            throw new IllegalArgumentException();
    }

    @Override
    public MeasureIngester<ExBiTopNCounter<ByteArray>> newIngester() {
        return new TopNMeasureIngester<ExItem<ByteArray>, ExBiTopNCounter<ByteArray>>() {

            protected ExBiTopNCounter<ByteArray> valueOf(String[] values) {
                // Construct key
                byte[] keyArray = new byte[keyLength];
                int offset = 0;
                ByteArray[] keys = new ByteArray[dimensionEncodings.length];
                for (int i = 0; i < dimensionEncodings.length; i++) {
                    keys[i] = new ByteArray(keyArray, offset, dimensionEncodings[i].getLengthOfEncoding());
                    if (values[i + 1] == null) {
                        Arrays.fill(keyArray, offset, offset + dimensionEncodings[i].getLengthOfEncoding(),
                                DimensionEncoding.NULL);
                    } else {
                        dimensionEncodings[i].encode(values[i + 1], keyArray, offset);
                    }
                    offset += dimensionEncodings[i].getLengthOfEncoding();
                }
                // Construct value
                double counter = values[0] == null ? 0 : Double.parseDouble(values[0]);

                ExBiTopNCounter<ByteArray> topNCounter = new ExBiTopNCounter<>(
                        dataType.getPrecision() * ExBiTopNCounter.EXTRA_SPACE_RATE, dimensionEncodings.length);
                topNCounter.offer(new ExItem.ExByteArrayItem(keys), counter);
                return topNCounter;
            }

            protected ExBiTopNCounter<ByteArray> reEncodeDictionary(ExBiTopNCounter<ByteArray> value) {
                ExBiTopNCounter<ByteArray> topNCounter = value;

                int topNSize = topNCounter.size();
                byte[] newKeyBuf = new byte[topNSize * newKeyLength];
                int offset = 0;
                for (Counter<ExItem<ByteArray>> c : topNCounter) {
                    for (int i = 0; i < newDimensionEncodings.length; i++) {
                        ByteArray keyArray = c.getItem().elems[i];
                        String dimValue = dimensionEncodings[i].decode(keyArray.array(), keyArray.offset(),
                                keyArray.length());

                        c.getItem().elems[i].reset(newKeyBuf, offset, newDimensionEncodings[i].getLengthOfEncoding());
                        newDimensionEncodings[i].encode(dimValue, newKeyBuf, offset);
                        offset += newDimensionEncodings[i].getLengthOfEncoding();
                    }
                }
                return topNCounter;
            }
        };
    }

    @Override
    protected IAdvMeasureFiller getTopNMeasureFiller(DimensionEncoding[] dimensionEncodings, int[] literalTupleIdx, int numericTupleIdx) {
        return new IAdvMeasureFiller() {
            private ExBiTopNCounter<ByteArray> topNCounter;
            private Iterator<Counter<ExItem<ByteArray>>> topNCounterIterator;
            private int expectRow = 0;

            @SuppressWarnings("unchecked")
            @Override
            public void reload(Object measureValue) {
                this.topNCounter = (ExBiTopNCounter<ByteArray>) measureValue;
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

                Counter<ExItem<ByteArray>> counter = topNCounterIterator.next();
                for (int i = 0; i < dimensionEncodings.length; i++) {
                    ByteArray byteArray = counter.getItem().elems[i];
                    String colValue = dimensionEncodings[i].decode(byteArray.array(), byteArray.offset(),
                            byteArray.length());
                    tuple.setDimensionValue(literalTupleIdx[i], colValue);
                }
                tuple.setMeasureValue(numericTupleIdx, counter.getCount());
                if (expectRow < 5) {
                    logger.debug("Fill tuple measure value at {} with {}", numericTupleIdx, counter.getCount());
                }
            }
        };
    }

    @Override
    public MeasureAggregator<ExBiTopNCounter<ByteArray>> newAggregator() {
        return new ExBiTopNAggregator();
    }
}
