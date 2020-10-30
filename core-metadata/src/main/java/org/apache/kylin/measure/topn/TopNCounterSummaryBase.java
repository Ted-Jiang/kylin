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
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Modified from the StreamSummary.java in https://github.com/addthis/stream-lib
 * <p>
 * Based on the <i>Space-Saving</i> algorithm and the <i>Stream-Summary</i>
 * data structure as described in:
 * <i>Efficient Computation of Frequent and Top-k Elements in Data Streams</i>
 * by Metwally, Agrawal, and Abbadi
 *
 * @param <T> type of data in the stream to be summarized
 */
public abstract class TopNCounterSummaryBase<T> implements ITopNCounterSummary<T> {

    protected final boolean descending;
    protected final Comparator<Counter> comparator;
    protected int capacity;
    protected Map<T, Counter<T>> counterMap;
    protected LinkedList<Counter<T>> counterSortedList; //a linked list, first the is the toppest element

    /**
     * @param capacity maximum size (larger capacities improve accuracy)
     */
    public TopNCounterSummaryBase(int capacity, boolean descending) {
        this.capacity = capacity;
        this.descending = descending;
        this.comparator = descending ? DESC_COMPARATOR : ASC_COMPARATOR;
        this.counterMap = Maps.newHashMap();
        this.counterSortedList = Lists.newLinkedList();
    }

    public abstract boolean isFull();

    protected abstract int getRetainThresholdForOffer();

    protected abstract int getRetainThresholdForMerge();

    protected abstract void retainSorted(int newCapacity);

    /**
     * It's for the merge process to estimate the count of removed elements
     */
    protected abstract double getCounterSummaryBoundary();

    /**
     * Check whether the item occurred in this counter summary
     */
    protected abstract boolean occur(T item);

    /**
     * This method will be called at the end of merge process
     */
    protected void retainUnsorted(int newCapacity) {
        sortAndRetain(newCapacity);
    }

    @Override
    public void sortAndRetain() {
        sortAndRetain(capacity);
    }

    /**
     * Sort and keep the expected size;
     */
    protected void sortAndRetain(int newCapacity) {
        sortUnsorted(newCapacity);
        retainSorted(newCapacity);
    }

    protected void sortUnsorted(int newCapacity) {
        if (ordered()) {
            return;
        }
        counterSortedList = Lists.newLinkedList(counterMap.values());
        Collections.sort(counterSortedList, comparator);
    }

    /**
     * Put element to the head position;
     * The consumer should call this method with count in ascending way; the item will be directly put to the head of the list, without comparison for best performance;
     *
     * @param item
     * @param count
     */
    public void offerToHead(T item, double count) {
        Counter<T> c = new Counter<>(item, count);
        counterMap.put(c.item, c);
        counterSortedList.addFirst(c);
    }

    @Override
    public void offer(T item) {
        offer(item, 1.0);
    }

    /**
     * Algorithm: <i>Space-Saving</i>
     *
     * @param item stream element (<i>e</i>)
     * @return false if item was already in the stream summary, true otherwise
     */
    @Override
    public void offer(T item, double incrementCount) {
        Counter<T> counterNode = counterMap.get(item);
        if (counterNode == null) {
            counterNode = new Counter<>(item, incrementCount);
            counterMap.put(item, counterNode);
        } else {
            counterNode.setCount(counterNode.getCount() + incrementCount);
        }
        if (size() > getRetainThresholdForOffer()) {
            retain(capacity);
            toUnordered();
        }
    }

    /**
     * Merge another counter into this counter;
     *
     * @param another0
     * @return
     */
    @Override
    public ITopNCounter<T> merge(ITopNCounter<T> another0) {
        Preconditions.checkArgument(another0 instanceof TopNCounterSummaryBase,
                "The class for another is " + another0.getClass() + " which should be " + this.getClass());

        TopNCounterSummaryBase<T> another = (TopNCounterSummaryBase<T>) another0;
        if (another.counterMap.isEmpty()) {
            return this;
        }

        // Find the estimation value for removed elements
        double e1 = getCounterSummaryBoundary();
        double e2 = another.getCounterSummaryBoundary();
        return merge(another, e1, e2);
    }

    protected ITopNCounter<T> merge(TopNCounterSummaryBase<T> another, double b1, double b2) {
        toUnordered();

        if (another.isFull()) {
            for (Counter<T> entry : counterMap.values()) {
                if (another.occur(entry.item)) {
                    entry.count += b2;
                }
            }
        }

        for (Counter<T> anotherEntry : another.counterMap.values()) {
            Counter<T> entry;
            if (occur(anotherEntry.item)) {
                entry = this.counterMap.get(anotherEntry.item);
                if (entry != null) {
                    entry.count += anotherEntry.count - b2;
                } else {
                    entry = new Counter<>(anotherEntry.item, anotherEntry.count + b1);
                    this.counterMap.put(anotherEntry.item, entry);
                }
            } else {
                entry = new Counter<>(anotherEntry.item, anotherEntry.count);
                this.counterMap.put(anotherEntry.item, entry);
            }
        }

        if (counterMap.size() > getRetainThresholdForMerge()) {
            retainUnsorted(capacity);
        }
        return this;
    }

    @Override
    public List<Counter<T>> topK(int k) {
        List<Counter<T>> topK = new ArrayList<>(k);
        Iterator<Counter<T>> iterator = iteratorForTopK();
        while (iterator.hasNext() && topK.size() < k) {
            Counter<T> b = iterator.next();
            topK.add(b);
        }

        return topK;
    }

    @Override
    public int getCapacity() {
        return capacity;
    }

    public boolean isDescending() {
        return descending;
    }

    @Override
    public int size() {
        return counterMap.size();
    }

    public boolean isEmpty() {
        return counterMap.isEmpty();
    }

    public boolean ordered() {
        return size() == 0 || !counterSortedList.isEmpty();
    }

    public void toUnordered() {
        // Empty ineffective containers, including LinkedList
        counterSortedList = Lists.newLinkedList();
    }

    /**
     * Get the counter values in ascending order
     *
     * @return
     */
    @Override
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
        sortAndRetain();
        return counterSortedList.descendingIterator();
    }

    protected Iterator<Counter<T>> iteratorForTopK() {
        sortAndRetain();
        return counterSortedList.iterator();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append('[');
        Iterator<Counter<T>> iterator = counterMap.values().iterator();
        while (iterator.hasNext()) {
            Counter<T> b = iterator.next();
            sb.append(b.item);
            sb.append(':');
            sb.append(b.count);
        }
        sb.append(']');
        return sb.toString();
    }
}