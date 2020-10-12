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
import org.apache.kylin.shaded.com.google.common.collect.Lists;
import org.apache.kylin.shaded.com.google.common.collect.Maps;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class TopNPreciseCounter<T> implements ITopNCounter<T> {
    protected Map<T, Counter<T>> counterMap;
    protected LinkedList<Counter<T>> counterSortedList; //a linked list, first the is the toppest element
    protected boolean descending;

    protected boolean ordered = true;

    public TopNPreciseCounter() {
        this(true);
    }

    public TopNPreciseCounter(boolean descending) {
        this.counterMap = Maps.newHashMap();
        this.counterSortedList = Lists.newLinkedList();
        this.descending = descending;
    }

    public int size() {
        return counterMap.size();
    }

    public void offer(T item) {
        offer(item, 1.0);
    }

    public void offer(T item, double incrementCount) {
        Counter<T> counterNode = counterMap.get(item);
        if (counterNode == null) {
            counterNode = new Counter<T>(item, incrementCount);
            counterMap.put(item, counterNode);
        } else {
            counterNode.setCount(counterNode.getCount() + incrementCount);
        }
        ordered = false;
    }

    public void sort() {
        if (!ordered) {
            counterSortedList = Lists.newLinkedList(counterMap.values());
            Collections.sort(counterSortedList, this.descending ? DESC_COMPARATOR : ASC_COMPARATOR);
            ordered = true;
        }
    }

    public ITopNCounter<T> merge(ITopNCounter<T> another0) {
        Preconditions.checkArgument(another0 instanceof TopNPreciseCounter,
                "The class for another is " + another0.getClass() + " which should be " + this.getClass());

        TopNPreciseCounter<T> another = (TopNPreciseCounter<T>) another0;
        if (another.size() == 0) {
            return this;
        }

        for (Map.Entry<T, Counter<T>> entry : another.counterMap.entrySet()) {
            Counter<T> counter = this.counterMap.get(entry.getKey());
            if (counter != null) {
                counter.setCount(counter.getCount() + entry.getValue().count);
            } else {
                counter = new Counter<>(entry.getValue().getItem(), entry.getValue().count);
                this.counterMap.put(entry.getValue().getItem(), counter);
            }
        }

        ordered = false;

        return this;
    }

    public List<Counter<T>> topK(int k) {
        List<Counter<T>> topK = new ArrayList<>(k);
        Iterator<Counter<T>> iterator = getIterator(false);
        while (iterator.hasNext()) {
            Counter<T> b = iterator.next();
            if (topK.size() == k) {
                return topK;
            }
            topK.add(b);
        }

        return topK;
    }

    /**
     * Get the counter values in ascending order which will be used for better serialization
     *
     * @return
     */
    public double[] getCounters() {
        double[] counters = new double[size()];
        int index = 0;

        Iterator<Counter<T>> iterator = iterator();
        while (iterator.hasNext()) {
            Counter<T> b = iterator.next();
            counters[index] = b.count;
            index++;
        }

        assert index == size();
        return counters;
    }

    @Override
    public Iterator<Counter<T>> iterator() {
        return getIterator(true);
    }

    private Iterator<Counter<T>> getIterator(boolean reverseOrder) {
        sort();
        if (reverseOrder) {
            return this.counterSortedList.descendingIterator();
        } else {
            return this.counterSortedList.iterator();
        }
    }

    /**
     * For TopNAggregator to avoid concurrency issues
     *
     * @return
     */
    public TopNPreciseCounter<T> copy() {
        TopNPreciseCounter result = new TopNPreciseCounter();
        result.counterMap = Maps.newHashMap(counterMap);
        return result;
    }
}
