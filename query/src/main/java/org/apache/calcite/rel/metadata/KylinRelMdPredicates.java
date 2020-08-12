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

package org.apache.calcite.rel.metadata;

import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;

import com.google.common.collect.ImmutableList;

public class KylinRelMdPredicates extends RelMdPredicates {

    /** Infers predicates for a {@link org.apache.calcite.rel.core.Join}. */
    public RelOptPredicateList getPredicates(Join join, ImmutableList<RexNode> leftFilters,
            ImmutableList<RexNode> rightFilters, RelMetadataQuery mq) {
        RexBuilder rB = join.getCluster().getRexBuilder();
        RelNode left = join.getInput(0);
        RelNode right = join.getInput(1);

        final RelOptPredicateList leftInfo = mq.getPulledUpPredicates(left);
        final RelOptPredicateList rightInfo = mq.getPulledUpPredicates(right);

        ImmutableList.Builder<RexNode> leftFiltersCombined = ImmutableList.builder();
        ImmutableList.Builder<RexNode> rightFiltersCombined = ImmutableList.builder();

        leftFiltersCombined.addAll(leftFilters).addAll(leftInfo.pulledUpPredicates);
        rightFiltersCombined.addAll(rightFilters).addAll(rightInfo.pulledUpPredicates);

        JoinConditionBasedPredicateInference jI = new JoinConditionBasedPredicateInference(join,
                RexUtil.composeConjunction(rB, leftFiltersCombined.build(), false),
                RexUtil.composeConjunction(rB, rightFiltersCombined.build(), false));

        return jI.inferPredicates(true);
    }
}
