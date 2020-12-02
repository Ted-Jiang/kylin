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

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.measure.MeasureAggregator;

public abstract class TopNAggregatorBase<K, T extends ITopNCounterSummary<K>> extends MeasureAggregator<T> {
    protected int capacity = 0;
    protected T sum = null;

    protected abstract T getEmptyCounter(T template, int capacity);

    @Override
    public void reset() {
        sum = null;
    }

    @Override
    public void aggregate(T value) {
        if (sum == null) {
            capacity = value.getCapacity();
            KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
            sum = getEmptyCounter(value, capacity * kylinConfig.getTopNCapacityFactorForQuerySum());
        }
        sum.merge(value);
    }

    @Override
    public T aggregate(T value1, T value2) {
        int thisCapacity = value1.getCapacity();
        T aggregated = getEmptyCounter(value1, thisCapacity * 2);
        aggregated.merge(value1);
        aggregated.merge(value2);
        aggregated.retain(thisCapacity);
        return aggregated;
    }

    @Override
    public T getState() {
        sum.retain(capacity);
        return sum;
    }

    @Override
    public int getMemBytesEstimate() {
        return 8 * capacity / 4;
    }

    public abstract TopNAggregatorBase copy();

}
