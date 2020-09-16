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
public abstract class TopNCounterSummary<T> implements ITopNCounter<T> {

    protected int capacity;
    protected Map<T, Counter<T>> counterMap;
    protected LinkedList<Counter<T>> counterSortedList; //a linked list, first the is the toppest element

    protected boolean descending;

    public TopNCounterSummary(int capacity) {
        this(capacity, true);
    }

    /**
     * @param capacity maximum size (larger capacities improve accuracy)
     */
    public TopNCounterSummary(int capacity, boolean descending) {
        this.capacity = capacity;
        this.descending = descending;
        this.counterMap = Maps.newHashMap();
        this.counterSortedList = Lists.newLinkedList();
    }

    /**
     * This method will be called at the end of merge process
     */
    protected abstract void retainUnsorted(int newCapacity);

    /**
     * It's for the merge process to estimate the count of removed elements
     */
    protected abstract double getEstimationOfRemoved();

    /**
     * Merge another with e1 for this estimation of removed and e2 for another estimation of removed
     */
    protected abstract ITopNCounter<T> merge(TopNCounterSummary<T> another, double e1, double e2);

    /**
     * Retain the capacity to the given number; The extra counters will be cut off
     *
     * @param newCapacity
     */
    public void retain(int newCapacity) {
        assert newCapacity > 0;

        this.capacity = newCapacity;
        if (size() > newCapacity) {
            sortAndRetain(newCapacity);
        }
    }

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

    /**
     * Sort all elements and fill counterSortedList
     */
    protected void sortUnsorted(int newCapacity) {
        if (ordered()) {
            return;
        }
        counterSortedList = Lists.newLinkedList(counterMap.values());
        Collections.sort(counterSortedList, this.descending ? DESC_COMPARATOR : ASC_COMPARATOR);
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
            counterNode = new Counter<T>(item, incrementCount);
            counterMap.put(item, counterNode);
        } else {
            counterNode.setCount(counterNode.getCount() + incrementCount);
        }
        if (size() >= capacity * 2) {
            retain(capacity);
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
        Preconditions.checkArgument(another0 instanceof TopNCounterSummary,
                "The class for another is " + another0.getClass() + " which should be " + this.getClass());

        TopNCounterSummary<T> another = (TopNCounterSummary<T>) another0;
        if (another.size() == 0) {
            return this;
        }

        // Find the estimation value for removed elements
        double e1 = getEstimationOfRemoved();
        double e2 = another.getEstimationOfRemoved();
        return merge(another, e1, e2);
    }

    @Override
    public List<Counter<T>> topK(int k) {
        List<Counter<T>> topK = new ArrayList<>(k);
        Iterator<Counter<T>> iterator = getIterator(false);
        while (iterator.hasNext() && topK.size() < k) {
            Counter<T> b = iterator.next();
            topK.add(b);
        }

        return topK;
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

    public int getCapacity() {
        return capacity;
    }

    /**
     * @return number of items stored
     */
    public int size() {
        return counterMap.size();
    }

    /**
     * Result may be not sorted
     *
     * @return
     */
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

    @Override
    public Iterator<Counter<T>> iterator() {
        return getIterator(true);
    }

    private Iterator<Counter<T>> getIterator(boolean reverseOrder) {
        sortAndRetain();
        if (reverseOrder) {
            return this.counterSortedList.descendingIterator();
        } else {
            return this.counterSortedList.iterator();
        }
    }

    private void retainSorted(int newCapacity) {
        this.capacity = newCapacity;
        if (this.size() > newCapacity) {
            Counter<T> toRemoved;
            for (int i = 0, n = this.size() - newCapacity; i < n; i++) {
                toRemoved = counterSortedList.pollLast();
                this.counterMap.remove(toRemoved.item);
            }
        }
    }

    protected boolean ordered() {
        return size() == 0 || !counterSortedList.isEmpty();
    }
}