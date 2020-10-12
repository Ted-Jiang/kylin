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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

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

    public static final int EXTRA_SPACE_RATE = 50;

    protected CounterMap<T> counterMap;

    public TopNCounterSummary(int capacity) {
        this(capacity, true);
    }

    /**
     * @param capacity maximum size (larger capacities improve accuracy)
     */
    public TopNCounterSummary(int capacity, boolean descending) {
        this.counterMap = new CounterMap<>(capacity, descending);
    }

    /**
     * This method will be called at the end of merge process
     */
    protected abstract void retainUnsorted(int newCapacity);

    /**
     * It's for the merge process to estimate the count of removed elements
     */
    protected abstract double getCounterSummaryBoundary();

    /**
     * Check whether the item occurred in this counter summary
     */
    protected abstract boolean occur(T item);

    /**
     * Retain the capacity to the given number; The extra counters will be cut off
     *
     * @param newCapacity
     */
    public void retain(int newCapacity) {
        counterMap.retain(newCapacity);
    }

    public void sortAndRetain() {
        counterMap.sortAndRetain();
    }

    /**
     * Sort and keep the expected size;
     */
    protected void sortAndRetain(int newCapacity) {
        sortUnsorted(newCapacity);
        counterMap.retainSorted(newCapacity);
    }

    protected void sortUnsorted(int newCapacity) {
        counterMap.sort();
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
        counterMap.offerToHead(c);
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
            counterMap.offer(counterNode);
        } else {
            counterNode.setCount(counterNode.getCount() + incrementCount);
        }
        if (counterMap.size() >= counterMap.getCapacity() * 2) {
            counterMap.retain(counterMap.getCapacity());
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
        if (another.counterMap.isEmpty()) {
            return this;
        }

        // Find the estimation value for removed elements
        double e1 = getCounterSummaryBoundary();
        double e2 = another.getCounterSummaryBoundary();
        return merge(another, e1, e2);
    }

    /**
     * @param b1 when this is not full, it will be 0 and will not affect elements in another TopNCounterSummary
     * @param b2 when another is not full, it will be 0 and will not affect elements in this TopNCounterSummary
     */
    protected ITopNCounter<T> merge(TopNCounterSummary<T> another, double b1, double b2) {
        counterMap.toUnordered();

        if (another.counterMap.isFull()) {
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

        retainUnsorted(counterMap.getCapacity());
        return this;
    }

    @Override
    public List<Counter<T>> topK(int k) {
        List<Counter<T>> topK = new ArrayList<>(k);
        Iterator<Counter<T>> iterator = counterMap.descendingIterator();
        while (iterator.hasNext() && topK.size() < k) {
            Counter<T> b = iterator.next();
            topK.add(b);
        }

        return topK;
    }

    public int getCapacity() {
        return counterMap.getCapacity();
    }

    public boolean isDescending() {
        return counterMap.isDescending();
    }

    @Override
    public int size() {
        return counterMap.size();
    }

    /**
     * Get the counter values in ascending order
     *
     * @return
     */
    @Override
    public double[] getCounters() {
        return counterMap.getCounters();
    }

    @Override
    public String toString() {
        return counterMap.toString();
    }

    @Override
    public Iterator<Counter<T>> iterator() {
        return counterMap.iterator();
    }
}