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

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.dimension.DateDimEnc;
import org.apache.kylin.dimension.DictionaryDimEnc;
import org.apache.kylin.dimension.DimensionEncoding;
import org.apache.kylin.dimension.DimensionEncodingFactory;
import org.apache.kylin.measure.MeasureIngester;
import org.apache.kylin.measure.MeasureType;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.ParameterDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.realization.CapabilityResult;
import org.apache.kylin.metadata.realization.SQLDigest;
import org.apache.kylin.metadata.tuple.Tuple;
import org.apache.kylin.metadata.tuple.TupleInfo;
import org.apache.kylin.shaded.com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.kylin.metadata.realization.SQLDigest.OrderEnum.DESCENDING;

public abstract class TopNMeasureTypeBase<K, T extends ITopNCounterSummary<K>> extends MeasureType<T> {
    private static final Logger logger = LoggerFactory.getLogger(TopNMeasureTypeBase.class);

    public static final String FUNC_TOP_N = "TOP_N";

    public static final String CONFIG_ENCODING_PREFIX = "topn.encoding.";
    public static final String CONFIG_ENCODING_VERSION_PREFIX = "topn.encoding_version.";
    public static final String CONFIG_AGG = "topn.aggregation";
    public static final String CONFIG_ORDER = "topn.order";

    private boolean cuboidCanAnswer;

    protected final DataType dataType;

    public TopNMeasureTypeBase(DataType dataType) {
        // note at query parsing phase, the data type may be null, because only function and parameters are known
        this.dataType = dataType;
    }

    protected abstract void validateDataType(String dataType);

    protected abstract IAdvMeasureFiller getTopNMeasureFiller(DimensionEncoding[] dimensionEncodings, int[] literalTupleIdx, int numericTupleIdx);

    public void validate(FunctionDesc functionDesc) throws IllegalArgumentException {
        validate(functionDesc.getExpression(), functionDesc.getReturnDataType(), true);
    }

    private void validate(String funcName, DataType dataType, boolean checkDataType) {
        if (!FUNC_TOP_N.equals(funcName))
            throw new IllegalArgumentException();

        validateDataType(dataType.getName());

        if (dataType.getPrecision() < 1 || dataType.getPrecision() > 10000)
            throw new IllegalArgumentException();
    }

    @Override
    public boolean isMemoryHungry() {
        return true;
    }

    @Override
    public List<TblColRef> getColumnsNeedDictionary(FunctionDesc functionDesc) {
        List<TblColRef> columnsNeedDict = Lists.newArrayList();
        List<TblColRef> allCols = functionDesc.getParameter().getColRefs();
        int start = (functionDesc.getParameter().isColumnType() == true) ? 1 : 0;
        for (int i = start; i < allCols.size(); i++) {
            TblColRef tblColRef = allCols.get(i);
            String encoding = getEncoding(functionDesc, tblColRef).getFirst();
            if (StringUtils.isEmpty(encoding) || DictionaryDimEnc.ENCODING_NAME.equals(encoding)) {
                columnsNeedDict.add(tblColRef);
            }
        }

        return columnsNeedDict;
    }

    @Override
    public CapabilityResult.CapabilityInfluence influenceCapabilityCheck(Collection<TblColRef> unmatchedDimensions,
                                                                         Collection<FunctionDesc> unmatchedAggregations, SQLDigest digest, final MeasureDesc topN) {
        // TopN measure can (and only can) provide one numeric measure and one literal dimension
        // e.g. select seller, sum(gmv) from ... group by seller order by 2 desc limit 100

        cuboidCanAnswer = true; // true: have cuboid can answer query， false: no cuboid can answer query

        List<TblColRef> literalCol = getTopNLiteralColumn(topN.getFunction());
        for (TblColRef colRef : literalCol) {
            if (digest.filterColumns.contains(colRef) == true) {
                // doesn't allow filtering by topn literal column
                return null;
            }
        }

        if (digest.groupbyColumns.containsAll(literalCol) == false)
            return null;

        List retainList = unmatchedDimensions.stream().filter(colRef -> literalCol.contains(colRef)).collect(Collectors.toList());

        if (retainList.size() > 0) {
            cuboidCanAnswer = false;
        }

        // check digest requires only one measure
        if (digest.aggregations.size() == 1) {

            // the measure function must be SUM
            FunctionDesc onlyFunction = digest.aggregations.iterator().next();
            if (isTopNCompatibleSum(topN.getFunction(), onlyFunction) == false)
                return null;

            unmatchedDimensions.removeAll(literalCol);
            unmatchedAggregations.remove(onlyFunction);

            return new CapabilityResult.CapabilityInfluence() {
                @Override
                public double suggestCostMultiplier() {
                    if (totallyMatchTopN(digest)) {
                        return 0.3; // make sure TopN get ahead of other matched realizations
                    } else if (cuboidCanAnswer) {
                        return 1.3; // fuzzy topN match, but have cuboid can answer query
                    } else {
                        return 2;
                    }
                }

                @Override
                public MeasureDesc getInvolvedMeasure() {
                    return topN;
                }
            };
        }

        if (digest.aggregations.size() == 0) {
            // directly query the UHC column without sorting
            boolean b = unmatchedDimensions.removeAll(literalCol);
            if (b) {
                return new CapabilityResult.CapabilityInfluence() {
                    @Override
                    public double suggestCostMultiplier() {
                        return 2.0; // topn can answer but with a higher cost
                    }

                    @Override
                    public MeasureDesc getInvolvedMeasure() {
                        return topN;
                    }
                };
            }
        }

        return null;
    }

    private boolean checkSortAndOrder(List<TblColRef> sort, List<SQLDigest.OrderEnum> order) {
        return CollectionUtils.isNotEmpty(sort) && CollectionUtils.isNotEmpty(order) && sort.size() == order.size();
    }

    private boolean totallyMatchTopN(SQLDigest digest) {
        if (!checkSortAndOrder(digest.sortColumns, digest.sortOrders)) {
            return false;
        }

        TblColRef sortColumn = digest.sortColumns.get(0);

        // first sortUnsorted column must be sum()
        if (digest.groupbyColumns.contains(sortColumn)) {
            return false;
        }

        // first order must be desc
        if (!DESCENDING.equals(digest.sortOrders.get(0))) {
            return false;
        }

        if (!digest.hasLimit) {
            return false;
        }

        return true;
    }

    private boolean isTopNCompatibleSum(FunctionDesc topN, FunctionDesc sum) {
        if (sum == null)
            return false;

        if (!isTopN(topN))
            return false;

        TblColRef topnNumCol = getTopNNumericColumn(topN);

        if (topnNumCol == null) {
            if (sum.isCount())
                return true;

            return false;
        }

        if (sum.isSum() == false)
            return false;

        if (sum.getParameter() == null || sum.getParameter().getColRefs() == null
                || sum.getParameter().getColRefs().size() == 0)
            return false;

        TblColRef sumCol = sum.getParameter().getColRefs().get(0);
        return sumCol.equals(topnNumCol);
    }

    @Override
    public boolean needRewrite() {
        return false;
    }

    @Override
    public void adjustSqlDigest(List<MeasureDesc> measureDescs, SQLDigest sqlDigest) {
        // If sqlDiegest is already adjusted, then not to adjust it again.
        if (sqlDigest.isBorrowedContext) {
            return;
        }

        if (sqlDigest.aggregations.size() > 1) {
            return;
        }

        for (MeasureDesc measureDesc : measureDescs) {
            if (!sqlDigest.involvedMeasure.contains(measureDesc)) {
                continue;
            }
            FunctionDesc topnFunc = measureDesc.getFunction();
            List<TblColRef> topnLiteralCol = getTopNLiteralColumn(topnFunc);

            if (sqlDigest.groupbyColumns.containsAll(topnLiteralCol) == false) {
                continue;
            }

            if (sqlDigest.aggregations.size() > 0) {
                FunctionDesc origFunc = sqlDigest.aggregations.iterator().next();
                if (origFunc.isSum() == false && origFunc.isCount() == false) {
                    logger.warn("When query with topN, only SUM/Count function is allowed.");
                    return;
                }

                if (isTopNCompatibleSum(measureDesc.getFunction(), origFunc) == false) {
                    continue;
                }

                // topN not totally match, but have cuboid can answer, not use topN to adjust
                // topN totally match or (topN fuzzy match, but no cuboid can answer), use topN to adjust
                if (!totallyMatchTopN(sqlDigest) && cuboidCanAnswer) {
                    continue;
                }

                topnFunc.setIsDisableRewrite(origFunc.isDisableRewrite());
                logger.info("Rewrite function " + origFunc + " to " + topnFunc);
            }


            sqlDigest.aggregations = Lists.newArrayList(topnFunc);
            sqlDigest.groupbyColumns.removeAll(topnLiteralCol);
            sqlDigest.metricColumns.addAll(topnLiteralCol);
            break;
        }
    }

    @Override
    public boolean needAdvancedTupleFilling() {
        return true;
    }

    @Override
    public void fillTupleSimply(Tuple tuple, int indexInTuple, Object measureValue) {
        throw new UnsupportedOperationException();
    }

    @Override
    public IAdvMeasureFiller getAdvancedTupleFiller(FunctionDesc function, TupleInfo tupleInfo,
                                                    Map<TblColRef, Dictionary<String>> dictionaryMap) {
        final List<TblColRef> literalCols = getTopNLiteralColumn(function);
        final TblColRef numericCol = getTopNNumericColumn(function);
        final int[] literalTupleIdx = new int[literalCols.size()];
        final DimensionEncoding[] dimensionEncodings = getDimensionEncodings(function, literalCols, dictionaryMap);
        for (int i = 0; i < literalCols.size(); i++) {
            TblColRef colRef = literalCols.get(i);
            literalTupleIdx[i] = tupleInfo.hasColumn(colRef) ? tupleInfo.getColumnIndex(colRef) : -1;
        }

        // for TopN, the aggr must be SUM
        final int numericTupleIdx;
        if (numericCol != null) {
            if (function.isDisableRewrite()) {
                numericTupleIdx = tupleInfo.hasColumn(numericCol) ? tupleInfo.getColumnIndex(numericCol) : -1;
            } else {
                FunctionDesc sumFunc = FunctionDesc.newInstance(FunctionDesc.FUNC_SUM,
                        ParameterDesc.newInstance(numericCol), numericCol.getType().toString());
                String sumFieldName = sumFunc.getRewriteFieldName();
                numericTupleIdx = tupleInfo.hasField(sumFieldName) ? tupleInfo.getFieldIndex(sumFieldName)
                        : tupleInfo.hasColumn(numericCol) ? tupleInfo.getColumnIndex(numericCol) : -1;
            }
        } else {
            FunctionDesc countFunction = FunctionDesc.newInstance(FunctionDesc.FUNC_COUNT,
                    ParameterDesc.newInstance("1"), "bigint");
            numericTupleIdx = tupleInfo.getFieldIndex(countFunction.getRewriteFieldName());
        }
        return getTopNMeasureFiller(dimensionEncodings, literalTupleIdx, numericTupleIdx);
    }

    private static DimensionEncoding[] getDimensionEncodings(FunctionDesc function, List<TblColRef> literalCols,
                                                             Map<TblColRef, Dictionary<String>> dictionaryMap) {
        final DimensionEncoding[] dimensionEncodings = new DimensionEncoding[literalCols.size()];
        for (int i = 0; i < literalCols.size(); i++) {
            TblColRef colRef = literalCols.get(i);

            Pair<String, String> topNEncoding = TopNMeasureType.getEncoding(function, colRef);
            String encoding = topNEncoding.getFirst();
            String encodingVersionStr = topNEncoding.getSecond();
            if (StringUtils.isEmpty(encoding) || DictionaryDimEnc.ENCODING_NAME.equals(encoding)) {
                dimensionEncodings[i] = new DictionaryDimEnc(dictionaryMap.get(colRef));
            } else {
                int encodingVersion = 1;
                if (!StringUtils.isEmpty(encodingVersionStr)) {
                    try {
                        encodingVersion = Integer.parseInt(encodingVersionStr);
                    } catch (NumberFormatException e) {
                        throw new RuntimeException(TopNMeasureType.CONFIG_ENCODING_VERSION_PREFIX + colRef.getName()
                                + " has to be an integer");
                    }
                }
                Object[] encodingConf = DimensionEncoding.parseEncodingConf(encoding);
                String encodingName = (String) encodingConf[0];
                String[] encodingArgs = (String[]) encodingConf[1];

                encodingArgs = DateDimEnc.replaceEncodingArgs(encoding, encodingArgs, encodingName,
                        literalCols.get(i).getType());

                dimensionEncodings[i] = DimensionEncodingFactory.create(encodingName, encodingArgs, encodingVersion);
            }
        }

        return dimensionEncodings;
    }

    private static TblColRef getTopNNumericColumn(FunctionDesc functionDesc) {
        if (functionDesc.getParameter().isColumnType()) {
            return functionDesc.getParameter().getColRefs().get(0);
        }
        return null;
    }

    private static List<TblColRef> getTopNLiteralColumn(FunctionDesc functionDesc) {
        List<TblColRef> allColumns = functionDesc.getParameter().getColRefs();
        if (!functionDesc.getParameter().isColumnType()) {
            return allColumns;
        }
        return allColumns.subList(1, allColumns.size());
    }

    private boolean isTopN(FunctionDesc functionDesc) {
        return FUNC_TOP_N.equalsIgnoreCase(functionDesc.getExpression());
    }

    protected abstract class TopNMeasureIngester<K, T extends ITopNCounterSummary<K>> extends MeasureIngester<T> {
        protected DimensionEncoding[] dimensionEncodings = null;
        private List<TblColRef> literalCols = null;
        protected int keyLength = 0;

        protected volatile DimensionEncoding[] newDimensionEncodings = null;
        protected int newKeyLength = 0;
        private boolean needReEncode = true;

        protected abstract T valueOf(String[] values);

        protected abstract T reEncodeDictionary(T value);

        @Override
        public T valueOf(String[] values, MeasureDesc measureDesc,
                         Map<TblColRef, Dictionary<String>> dictionaryMap) {
            if (dimensionEncodings == null) {
                initialize(measureDesc, dictionaryMap);

                if (values.length != (literalCols.size() + 1)) {
                    throw new IllegalArgumentException();
                }
            }

            return valueOf(values);
        }

        @Override
        public T reEncodeDictionary(T value, MeasureDesc measureDesc,
                                    Map<TblColRef, Dictionary<String>> oldDicts, Map<TblColRef, Dictionary<String>> newDicts) {

            if (newDimensionEncodings == null) {
                synchronized (MeasureIngester.class) {
                    if (newDimensionEncodings == null) {
                        initialize(measureDesc, oldDicts, newDicts);
                    }
                }
            }

            if (!needReEncode) {
                // no need re-encode
                return value;
            }

            return reEncodeDictionary(value);
        }

        @Override
        public void reset() {
        }

        private void initialize(MeasureDesc measureDesc, Map<TblColRef, Dictionary<String>> dicts) {
            literalCols = getTopNLiteralColumn(measureDesc.getFunction());
            dimensionEncodings = getDimensionEncodings(measureDesc.getFunction(), literalCols, dicts);
            keyLength = 0;
            for (DimensionEncoding encoding : dimensionEncodings) {
                keyLength += encoding.getLengthOfEncoding();
                if (encoding instanceof DictionaryDimEnc) {
                    needReEncode = true;
                }
            }
        }

        private void initialize(MeasureDesc measureDesc, Map<TblColRef, Dictionary<String>> oldDicts,
                                Map<TblColRef, Dictionary<String>> newDicts) {
            initialize(measureDesc, oldDicts);

            newDimensionEncodings = getDimensionEncodings(measureDesc.getFunction(), literalCols, newDicts);
            newKeyLength = 0;
            for (DimensionEncoding encoding : newDimensionEncodings) {
                newKeyLength += encoding.getLengthOfEncoding();
            }
        }
    }

    /**
     * Get the encoding name and version for the given col from Measure FunctionDesc
     *
     * @param functionDesc
     * @param tblColRef
     * @return a pair of the encoding name and encoding version
     */
    public static final Pair<String, String> getEncoding(FunctionDesc functionDesc, TblColRef tblColRef) {
        String encoding = functionDesc.getConfiguration().get(CONFIG_ENCODING_PREFIX + tblColRef.getIdentity());
        String encodingVersion = functionDesc.getConfiguration()
                .get(CONFIG_ENCODING_VERSION_PREFIX + tblColRef.getIdentity());
        if (StringUtils.isEmpty(encoding)) {
            // for backward compatibility
            encoding = functionDesc.getConfiguration().get(CONFIG_ENCODING_PREFIX + tblColRef.getName());
            encodingVersion = functionDesc.getConfiguration().get(CONFIG_ENCODING_VERSION_PREFIX + tblColRef.getName());
        }

        return new Pair<>(encoding, encodingVersion);
    }
}
