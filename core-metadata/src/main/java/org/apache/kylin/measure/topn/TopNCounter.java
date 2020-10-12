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

import org.apache.kylin.shaded.com.google.common.base.Preconditions;

/**
 * Reduce the chance to call retain method
 *
 * @param <T> type of data in the stream to be summarized
 */
public class TopNCounter<T> extends TopNCounterDescending<T> {

    private Counter<T> minCounter = null;

    /**
     * @param capacity maximum size (larger capacities improve accuracy)
     */
    public TopNCounter(int capacity) {
        super(capacity);
    }

    @Override
    public void offer(T item, double incrementCount) {
        super.offer(item, incrementCount);
        minCounter = null;
    }

    @Override
    public ITopNCounter<T> merge(ITopNCounter<T> another0) {
        Preconditions.checkArgument(another0 instanceof TopNCounter,
                "The class for another is " + another0.getClass() + " which should be " + this.getClass());

        TopNCounter<T> another = (TopNCounter<T>) another0;
        if (another.counterMap.isEmpty()) {
            return this;
        }

        // Find the minimum value
        double m1 = getCounterSummaryBoundary();
        double m2 = another.getCounterSummaryBoundary();

        Counter<T> mCounter = null;
        if (minCounter != null && !another.counterMap.containsKey(minCounter.item)) {
            mCounter = minCounter;
        }
        if (another.minCounter != null && !counterMap.containsKey(another.minCounter.item)) {
            if (mCounter == null || mCounter.count > another.minCounter.count) {
                mCounter = another.minCounter;
            }
        }

        ITopNCounter<T> ret = merge(another, m1, m2);
        minCounter = mCounter != null ? counterMap.get(mCounter.item) : null;

        return ret;
    }

    @Override
    protected void retainUnsorted(int newCapacity) {
        if (counterMap.size() > newCapacity * 2) {
            sortAndRetain();
        }
    }

    @Override
    protected double getMinimum() {
        if (counterMap.ordered()) {
            minCounter = counterMap.getLast();
        }
        if (!counterMap.isFull()) {
            return 0.0;
        }
        if (minCounter != null) {
            return minCounter.count;
        }
        for (Counter<T> entry : counterMap.values()) {
            if (minCounter == null || minCounter.count > entry.count) {
                minCounter = entry;
            }
        }
        return minCounter != null ? minCounter.count : 0.0;
    }

    @Override
    public TopNCounter<T> copy() {
        TopNCounter result = new TopNCounter(getCapacity());
        result.counterMap = counterMap.copy();
        return result;
    }
}