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

public abstract class TopNCounterSummary<T> extends TopNCounterSummaryBase<T> {

    public TopNCounterSummary(int capacity) {
        this(capacity, true);
    }

    public TopNCounterSummary(int capacity, boolean descending) {
        super(capacity, descending);
    }

    @Override
    public boolean isFull() {
        return size() >= capacity;
    }

    @Override
    protected int getRetainThresholdForOffer() {
        return capacity * 2;
    }

    @Override
    protected int getRetainThresholdForMerge() {
        return capacity * 2;
    }

    @Override
    public void retain(int newCapacity) {
        assert newCapacity > 0;

        this.capacity = newCapacity;
        if (size() > capacity) {
            sortAndRetain();
        }
    }

    @Override
    protected void retainSorted(int newCapacity) {
        this.capacity = newCapacity;
        if (this.size() > capacity) {
            Counter<T> toRemoved;
            for (int i = 0, n = this.size() - newCapacity; i < n; i++) {
                toRemoved = counterSortedList.pollLast();
                this.counterMap.remove(toRemoved.item);
            }
        }
    }
}