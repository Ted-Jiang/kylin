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
import org.apache.kylin.common.util.MathUtil;
import org.apache.kylin.shaded.com.google.common.collect.Lists;

import java.util.Iterator;

public abstract class BiTopNCounterSummary<T> extends TopNCounterSummaryBase<T> {

    private Counter<T> hint;

    public BiTopNCounterSummary(int capacity) {
        super(capacity, true);
    }

    private int getBiCapacity() {
        return capacity * 2;
    }

    @Override
    public boolean isFull() {
        return size() >= getBiCapacity();
    }

    @Override
    protected int getRetainThresholdForOffer() {
        return getBiCapacity() * 2;
    }

    @Override
    protected int getRetainThresholdForMerge() {
        return KylinConfig.getInstanceFromEnv().getTopNRetainRowCountForMerge();
    }

    @Override
    public void retain(int newCapacity) {
        assert newCapacity > 0;

        this.capacity = newCapacity;
        if (size() > getBiCapacity()) {
            sortAndRetain();
        }
    }

    @Override
    protected void retainSorted(int newCapacity) {
        this.capacity = newCapacity;
        if (this.size() > getBiCapacity()) {
            //Remove the middle values
            Iterator<Counter<T>> iterator = counterSortedList.listIterator(capacity);
            int len = this.size() - getBiCapacity();
            for (int i = 0; i < len; i++) {
                Counter<T> toRemoved = iterator.next();
                iterator.remove();
                this.counterMap.remove(toRemoved.item);
            }
        }
    }

    protected double getCounterSummaryBoundary() {
        if (!isFull()) {
            return 0.0;
        }
        double medianValue;
        if (ordered()) {
            medianValue = getMedianValueFromOrdered();
        } else {
            KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
            if (kylinConfig.getTopNFindKthElementAlgorithm() == 1) {
                hint = MathUtil.findMedianElement(comparator, Lists.newArrayList(counterMap.values()), hint);
                medianValue = hint.count;
            } else {
                sortUnsorted(capacity);
                medianValue = getMedianValueFromOrdered();
            }
        }
        return medianValue;
    }

    private double getMedianValueFromOrdered() {
        Counter<T> highMinCounter = counterSortedList.get(capacity - 1);
        Counter<T> lowMaxCounter = counterSortedList.get(counterSortedList.size() - capacity);
        return (highMinCounter.count + lowMaxCounter.count) / 2;
    }
}
