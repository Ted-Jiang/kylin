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

package org.apache.kylin.query.optrule;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlMinMaxAggFunction;
import org.apache.calcite.sql.fun.SqlSumAggFunction;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBitSet;

import com.google.common.collect.ImmutableList;

/**
 * For case like:
 *      select A, sum(C)
 *      from (
 *          select A, B, sum(C) as C
 *          from T
 *          group by A, B
 *      )
 *      where D = ...
 *      group by A
 *
 * Firstly, with rule FilterAggregateTransposeRule & KylinFilterProjectTransposeRule, it will be transformed to
 *      select A, sum(C)
 *      from (
 *          select A, B, sum(C) as C
 *          from T
 *          where D = ...
 *          group by A, B
 *      )
 *      group by A
 *
 * Then it will match the rule pattern: (aggregate -> project -> aggregate)
 *                                                      ↓
 * by this rule, it will be reduced to              (aggregate)
 */

public class AggregateProjectAggregateReduceRule extends RelOptRule {

    public static final AggregateProjectAggregateReduceRule INSTANCE = new AggregateProjectAggregateReduceRule(//
            operand(LogicalAggregate.class, null, Aggregate.IS_SIMPLE, //
                    operand(LogicalProject.class, //
                            operand(LogicalAggregate.class, null, Aggregate.IS_SIMPLE, //
                                    operand(RelNode.class, any())))), //
            RelFactories.LOGICAL_BUILDER, "AggregateProjectAggregateReduceRule");

    private static final Map<Class<? extends SqlAggFunction>, Boolean> SUPPORTED_AGGREGATES = new IdentityHashMap<>();

    static {
        SUPPORTED_AGGREGATES.put(SqlMinMaxAggFunction.class, true);
        SUPPORTED_AGGREGATES.put(SqlSumAggFunction.class, true);
    }

    private AggregateProjectAggregateReduceRule(RelOptRuleOperand operand, RelBuilderFactory factory,
            String description) {
        super(operand, factory, description);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalAggregate aggOuter = call.rel(0);
        LogicalProject project = call.rel(1);
        LogicalAggregate aggInner = call.rel(2);

        // 1. check aggregate
        if (aggInner.getGroupSets().size() > 1 || aggOuter.getGroupSets().size() > 1) {
            return;
        }
        List<AggregateCall> aggCallAllList = new ArrayList<>();
        aggCallAllList.addAll(aggInner.getAggCallList());
        aggCallAllList.addAll(aggOuter.getAggCallList());
        for (AggregateCall aggCall : aggCallAllList) {
            if (!SUPPORTED_AGGREGATES.containsKey(aggCall.getAggregation().getClass())) {
                return;
            }
        }

        // 2. check aggregate on project on aggregate
        // 2.1 check group by
        ImmutableBitSet.Builder groupSetInnerBuilder = ImmutableBitSet.builder();
        for (int groupIdx : aggOuter.getGroupSet()) {
            RexNode exp = project.getProjects().get(groupIdx);
            if (!(exp instanceof RexInputRef)) {
                // Cannot handle "GROUP BY expression"
                return;
            }
            RexInputRef inputRef = (RexInputRef) exp;
            int groupIdxInner = inputRef.getIndex();
            if (groupIdxInner >= aggInner.getGroupCount()) {
                return;
            }
            int newGroupIdx = aggInner.getGroupSet().nth(groupIdxInner);
            groupSetInnerBuilder.set(newGroupIdx);
        }
        // 2.2 check aggregation
        ImmutableList.Builder<AggregateCall> aggCallListBuilder = ImmutableList.builder();
        for (AggregateCall aggCallOuter : aggOuter.getAggCallList()) {
            SqlKind sqlKindOuter = aggCallOuter.getAggregation().getKind();
            if (aggCallOuter.getArgList().size() > 1) {
                return;
            }
            int aggArgIdx = aggCallOuter.getArgList().get(0);
            RexNode exp = project.getProjects().get(aggArgIdx);
            if (!(exp instanceof RexInputRef)) {
                // Cannot handle "AGG(expression)"
                return;
            }
            RexInputRef inputRef = (RexInputRef) exp;
            int aggInnerIdx = inputRef.getIndex() - aggInner.getGroupCount();
            if (aggInnerIdx < 0 || aggInnerIdx >= aggInner.getAggCallList().size()) {
                return;
            }
            AggregateCall aggCallInner = aggInner.getAggCallList().get(aggInnerIdx);
            SqlKind sqlKindInner = aggCallInner.getAggregation().getKind();
            if (!sqlKindOuter.equals(sqlKindInner)) {
                return;
            }
            AggregateCall newAggCall = AggregateCall.create(aggCallOuter.getAggregation(), aggCallOuter.isDistinct(),
                    aggCallOuter.isApproximate(), aggCallInner.getArgList(), aggCallInner.filterArg,
                    aggCallOuter.getType(), aggCallOuter.name);
            aggCallListBuilder.add(newAggCall);
        }

        // All check pass, do reduce: (aggregate -> project -> aggregate) => (aggregate)
        // create new group set
        final ImmutableBitSet newGroupSet = groupSetInnerBuilder.build();
        // mapping input ref in aggr calls and generate new aggr calls
        final ImmutableList<AggregateCall> newAggCalls = aggCallListBuilder.build();

        RelBuilder relBuilder = call.builder();
        relBuilder.push(aggInner.getInput());
        relBuilder.aggregate(relBuilder.groupKey(newGroupSet, null), newAggCalls);
        RelNode rel = relBuilder.build();

        call.transformTo(rel);
    }
}