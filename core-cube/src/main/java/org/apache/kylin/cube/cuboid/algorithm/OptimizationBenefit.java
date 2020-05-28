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

package org.apache.kylin.cube.cuboid.algorithm;

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;

import org.apache.kylin.common.persistence.JsonSerializer;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.common.persistence.Serializer;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.metadata.MetadataConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;

@SuppressWarnings("serial")
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class OptimizationBenefit extends RootPersistentEntity {

    private static final Logger logger = LoggerFactory.getLogger(OptimizationBenefit.class);

    public static final String SCORE_TYPE = "opt_benefit";

    public static final Serializer<OptimizationBenefit> CUBE_SCORE_SERIALIZER = new JsonSerializer<>(
            OptimizationBenefit.class);

    public static OptimizationBenefit loadFromStore(ResourceStore store, CubeInstance cubeInstance) throws IOException {
        return store.getResource(getScorePath(cubeInstance), CUBE_SCORE_SERIALIZER);
    }

    public static void saveToStore(ResourceStore store, CubeInstance cubeInstance, OptimizationBenefit obj)
            throws IOException {
        store.putBigResource(getScorePath(cubeInstance), obj, System.currentTimeMillis(), CUBE_SCORE_SERIALIZER);
    }

    public static void removeFromStore(ResourceStore store, CubeInstance cubeInstance) throws IOException {
        store.deleteResource(getScorePath(cubeInstance));
    }

    private static String getScorePath(CubeInstance cubeInstance) {
        return ResourceStore.CUBE_SCORE_ROOT + "/" + SCORE_TYPE + "/" + cubeInstance.getName()
                + MetadataConstants.FILE_SURFIX;
    }

    public static final OptimizationBenefit ZERO = new OptimizationBenefit();

    // queryBenefit = (rollupBenefit - rollupCost) / totalRollupInputCount
    @JsonProperty("query_benefit")
    private double queryBenefit;

    @JsonProperty("rollup_benefit")
    private double rollupBenefit;

    @JsonProperty("rollup_cost")
    private double rollupCost;

    @JsonProperty("rollup_input_count")
    private double rollupInputCount;

    // spaceBenefit = (curTotalSize - recomTotalSize) / spaceLimit
    @JsonProperty("space_benefit")
    private double spaceBenefit;

    @JsonProperty("recommend_total_size")
    private double recomTotalSize;

    @JsonProperty("current_total_size")
    private double curTotalSize;

    @JsonProperty("space_limit_size")
    private double spaceLimit;

    // total benefit = query benefit + k * space benefit;
    @JsonProperty("space_benefit_ratio")
    private double k;

    @JsonProperty("total_benefit")
    private double totalBenefit;

    public OptimizationBenefit() {
        this.rollupBenefit = 0;
        this.rollupCost = 0;
        this.rollupInputCount = 0;
        this.queryBenefit = 0;

        this.recomTotalSize = 0;
        this.curTotalSize = 0;
        this.spaceLimit = 0;
        this.spaceBenefit = 0;

        this.k = 0;
        this.totalBenefit = 0;
    }

    public OptimizationBenefit(double rollupBenefit, double rollupCost, double rollupInputCount, double curTotalSize,
            double recomTotalSize, double spaceLimit) {
        this(rollupBenefit, rollupCost, rollupInputCount, curTotalSize, recomTotalSize, spaceLimit, 0.1);
    }

    public OptimizationBenefit(double rollupBenefit, double rollupCost, double rollupInputCount, double curTotalSize,
            double recomTotalSize, double spaceLimit, double spaceBenefitRatio) {
        updateRandomUuid();

        this.rollupBenefit = rollupBenefit;
        this.rollupCost = rollupCost;
        this.rollupInputCount = rollupInputCount;
        this.queryBenefit = rollupInputCount == 0 ? 0 : (rollupBenefit - rollupCost) / rollupInputCount;

        this.curTotalSize = curTotalSize;
        this.recomTotalSize = recomTotalSize;
        this.spaceLimit = spaceLimit;

        this.spaceBenefit = spaceLimit == 0 ? 0.0 : (curTotalSize - recomTotalSize) / spaceLimit;

        this.k = spaceBenefitRatio;
        this.totalBenefit = queryBenefit + k * spaceBenefit;
    }

    public double getTotalBenefit() {
        return totalBenefit;
    }

    public void setTotalBenefit(double totalBenefit) {
        this.totalBenefit = totalBenefit;
    }

    public double getK() {
        return k;
    }

    public void setK(double k) {
        this.k = k;
    }

    public double getSpaceLimit() {
        return spaceLimit;
    }

    public void setSpaceLimit(double spaceLimit) {
        this.spaceLimit = spaceLimit;
    }

    public double getCurTotalSize() {
        return curTotalSize;
    }

    public void setCurTotalSize(double curTotalSize) {
        this.curTotalSize = curTotalSize;
    }

    public double getSpaceBenefit() {
        return spaceBenefit;
    }

    public void setSpaceBenefit(double spaceBenefit) {
        this.spaceBenefit = spaceBenefit;
    }

    public double getRecomTotalSize() {
        return recomTotalSize;
    }

    public void setRecomTotalSize(double recomTotalSize) {
        this.recomTotalSize = recomTotalSize;
    }

    public double getRollupInputCount() {
        return rollupInputCount;
    }

    public void setRollupInputCount(double rollupInputCount) {
        this.rollupInputCount = rollupInputCount;
    }

    public double getRollupBenefit() {
        return rollupBenefit;
    }

    public void setRollupBenefit(double rollupBenefit) {
        this.rollupBenefit = rollupBenefit;
    }

    public double getRollupCost() {
        return rollupCost;
    }

    public void setRollupCost(double rollupCost) {
        this.rollupCost = rollupCost;
    }

    public double getQueryBenefit() {
        return queryBenefit;
    }

    public void setQueryBenefit(double queryBenefit) {
        this.queryBenefit = queryBenefit;
    }

    @JsonProperty("score")
    public double getScore() {
        if (isZero()) {
            return -1;
        }
        return (int) (100 * (totalBenefit <= 0 ? 1 : 1 / (1 + totalBenefit)));
    }

    public String getScoreHint() {
        if (isZero()) {
            return "";
        }
        String queryDesc = queryBenefit >= 0 ? "speed up " : "slow down ";
        String spaceDesc = spaceBenefit >= 0 ? "reduce " : "increase ";

        String queryRatio = String.format(Locale.ROOT, "%.0f", Math.abs(queryBenefit) * 100);
        String spaceRatio = String.format(Locale.ROOT, "%.0f", Math.abs(spaceBenefit) * 100);
        String spaceSize = String.format(Locale.ROOT, "%.0f", Math.abs(curTotalSize - recomTotalSize));

        StringBuilder sb = new StringBuilder();
        sb.append("Benefit you will get after optimization: \n");
        sb.append("- query latency: ").append(queryDesc).append(queryRatio).append("%\n");
        sb.append("- storage usage: ").append(spaceDesc).append(spaceRatio).append("% with size ").append(spaceSize)
                .append(" MB\n");
        return sb.toString();
    }

    public void printDetails() {
        logger.info("benefit = queryBenefitRatio + spaceBenefitRatio = {}", totalBenefit);
        logger.info("                    queryBenefit: {}", queryBenefit);
        logger.info("                    spaceBenefit: {}", spaceBenefit);
        logger.info("                   rollupBenefit: {}", rollupBenefit);
        logger.info("                      rollupCost: {}", rollupCost);
        logger.info("                rollupInputCount: {}", rollupInputCount);
        logger.info("                    curTotalSize: {}", curTotalSize);
        logger.info("                  recomTotalSize: {}", recomTotalSize);
        logger.info("                      spaceLimit: {}", spaceLimit);
        logger.info("               spaceBenefitRatio: {}", k);
    }

    public boolean isZero() {
        return this.equals(ZERO);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        if (!super.equals(o))
            return false;
        OptimizationBenefit that = (OptimizationBenefit) o;
        return Double.compare(that.queryBenefit, queryBenefit) == 0
                && Double.compare(that.rollupBenefit, rollupBenefit) == 0
                && Double.compare(that.rollupCost, rollupCost) == 0
                && Double.compare(that.rollupInputCount, rollupInputCount) == 0
                && Double.compare(that.spaceBenefit, spaceBenefit) == 0
                && Double.compare(that.recomTotalSize, recomTotalSize) == 0
                && Double.compare(that.curTotalSize, curTotalSize) == 0
                && Double.compare(that.spaceLimit, spaceLimit) == 0 && Double.compare(that.k, k) == 0
                && Double.compare(that.totalBenefit, totalBenefit) == 0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), queryBenefit, rollupBenefit, rollupCost, rollupInputCount, spaceBenefit,
                recomTotalSize, curTotalSize, spaceLimit, k, totalBenefit);
    }
}
