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

import java.util.Iterator;

public abstract class BiTopNCounterSummary<T> extends TopNCounterSummaryBase<T> {

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
        return getBiCapacity();
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
            int index = this.size() - getBiCapacity();
            Counter<T> toRemoved;
            while (index > 0) {
                toRemoved = iterator.next();
                iterator.remove();
                this.counterMap.remove(toRemoved.item);
                index--;
            }
        }
    }

    protected double getCounterSummaryBoundary() {
        if (!isFull()) {
            return 0.0;
        }
        sortUnsorted(capacity);
        Counter<T> highMinCounter = counterSortedList.get(capacity - 1);
        Counter<T> lowMaxCounter = counterSortedList.get(counterSortedList.size() - capacity);

        return (highMinCounter.count + lowMaxCounter.count) / 2;
    }
}
