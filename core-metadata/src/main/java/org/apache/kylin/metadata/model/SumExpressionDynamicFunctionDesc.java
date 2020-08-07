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

package org.apache.kylin.metadata.model;

import java.util.Set;

import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.expression.NullExpressionReplacer;
import org.apache.kylin.metadata.expression.TupleExpression;
import org.apache.kylin.shaded.com.google.common.collect.Sets;

public class SumExpressionDynamicFunctionDesc extends ExpressionDynamicFunctionDesc {

    public static final TblColRef mockCntCol = TblColRef.newInnerColumn(FunctionDesc.FUNC_COUNT,
            TblColRef.InnerDataTypeEnum.DERIVED);

    private Set<TblColRef> measureColumnSet;

    public SumExpressionDynamicFunctionDesc(ParameterDesc parameter, TupleExpression tupleExpression) {
        super(parameter, FUNC_SUM, null, tupleExpression.accept(new NullExpressionReplacer()));
    }

    @Override
    public String getRewriteFieldName() {
        return "_KY_" + FUNC_SUM + "_" + tupleExpression.toString();
    }

    @Override
    public DataType getRewriteFieldType() {
        return getReturnDataType();
    }

    @Override
    public Set<TblColRef> getRuntimeMeasures() {
        if (measureColumnSet == null) {
            measureColumnSet = Sets.newHashSet(super.getRuntimeMeasures());
            measureColumnSet.remove(mockCntCol);
        }
        return measureColumnSet;
    }

    @Override
    protected FunctionDesc constructRuntimeFunction(TblColRef column) {
        return column == mockCntCol ? FunctionDesc.newInstance(FunctionDesc.FUNC_COUNT, null, null)
                : FunctionDesc.newInstance(FUNC_SUM, ParameterDesc.newInstance(column), null);
    }

    @Override
    protected void resetReturnType() {
        DataType returnType = tupleExpression.getDataType();
        for (FunctionDesc funcDesc : runtimeFuncMap.values()) {
            returnType = TupleExpression.referDataType(returnType, funcDesc.getReturnDataType());
        }
        setReturnDataType(returnType);
    }
}