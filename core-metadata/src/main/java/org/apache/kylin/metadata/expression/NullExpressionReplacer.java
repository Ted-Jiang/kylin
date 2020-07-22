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

package org.apache.kylin.metadata.expression;

import java.util.List;

import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.shaded.com.google.common.collect.Lists;

public class NullExpressionReplacer implements ExpressionVisitor {

    private TupleExpression replacementTupleExpression;

    public NullExpressionReplacer() {
        replacementTupleExpression = ConstantTupleExpression.ZERO;
    }

    public TupleExpression visitConstant(ConstantTupleExpression constExpr) {
        return constExpr;
    }

    public TupleExpression visitColumn(ColumnTupleExpression colExpr) {
        return colExpr;
    }

    public TupleExpression visitBinary(BinaryTupleExpression binaryExpr) {
        TupleExpression leftCopy = getReplacementTupleExpression(binaryExpr.getLeft(), binaryExpr.getDataType());
        TupleExpression rightCopy = getReplacementTupleExpression(binaryExpr.getRight(), binaryExpr.getDataType());
        return new BinaryTupleExpression(binaryExpr.getDataType(), binaryExpr.getOperator(),
                Lists.newArrayList(leftCopy, rightCopy));
    }

    public TupleExpression visitCaseCall(CaseTupleExpression caseExpr) {
        List<Pair<TupleFilter, TupleExpression>> whenList = Lists
                .newArrayListWithExpectedSize(caseExpr.getWhenList().size());
        for (Pair<TupleFilter, TupleExpression> entry : caseExpr.getWhenList()) {
            TupleFilter filter = entry.getFirst();
            TupleExpression expression = getReplacementTupleExpression(entry.getSecond(), caseExpr.getDataType());
            whenList.add(new Pair<>(filter, expression));
        }
        TupleExpression elseExpr = getReplacementTupleExpression(caseExpr.getElseExpr(), caseExpr.getDataType());
        return new CaseTupleExpression(caseExpr.getDataType(), whenList, elseExpr);
    }

    public TupleExpression visitRexCall(RexCallTupleExpression rexCallExpr) {
        throw new UnsupportedOperationException();
    }

    public TupleExpression visitNone(NoneTupleExpression noneExpr) {
        return noneExpr;
    }

    private TupleExpression getReplacementTupleExpression(TupleExpression tupleExpression, DataType dataType) {
        if (tupleExpression == null || (tupleExpression instanceof ConstantTupleExpression
                && ((ConstantTupleExpression) tupleExpression).getValue() == null)) {
            if (dataType.isNumberFamily() && !dataType.isDecimal()) {
                return replacementTupleExpression;
            } else {
                return tupleExpression;
            }
        } else {
            return tupleExpression.accept(this);
        }
    }
}
