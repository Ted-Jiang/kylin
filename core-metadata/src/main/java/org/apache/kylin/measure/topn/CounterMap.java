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

import org.apache.kylin.shaded.com.google.common.collect.Lists;
import org.apache.kylin.shaded.com.google.common.collect.Maps;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;

import static org.apache.kylin.measure.topn.ITopNCounter.ASC_COMPARATOR;
import static org.apache.kylin.measure.topn.ITopNCounter.DESC_COMPARATOR;

public class CounterMap<T> implements Map<T, Counter<T>>, ICounterSortedMap<T> {
    private int capacity;
    private Map<T, Counter<T>> counterMap;
    private LinkedList<Counter<T>> counterSortedList; //a linked list, first the is the toppest element

    private boolean descending;

    public CounterMap(int capacity, boolean descending) {
        this.capacity = capacity;
        this.descending = descending;
        this.counterMap = Maps.newHashMap();
        this.counterSortedList = Lists.newLinkedList();
    }

    @Override
    public int size() {
        return counterMap.size();
    }

    @Override
    public boolean isEmpty() {
        return counterMap.isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
        return counterMap.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        return counterMap.containsValue(value);
    }

    @Override
    public Counter<T> get(Object key) {
        return counterMap.get(key);
    }

    @Override
    public Counter<T> put(T key, Counter<T> value) {
        return counterMap.put(key, value);
    }

    @Override
    public Counter<T> remove(Object key) {
        return counterMap.remove(key);
    }

    @Override
    public void putAll(Map<? extends T, ? extends Counter<T>> m) {
        counterMap.putAll(m);
    }

    @Override
    public void clear() {
        counterMap.clear();
        counterSortedList.clear();
    }

    @Override
    public Set<T> keySet() {
        return counterMap.keySet();
    }

    @Override
    public Collection<Counter<T>> values() {
        return counterMap.values();
    }

    @Override
    public Set<Entry<T, Counter<T>>> entrySet() {
        return counterMap.entrySet();
    }

    @Override
    public boolean isDescending() {
        return descending;
    }

    public Iterator<Counter<T>> descendingIterator() {
        sortAndRetain();
        return counterSortedList.iterator();
    }

    public Iterator<Counter<T>> iterator() {
        sortAndRetain();
        return counterSortedList.descendingIterator();
    }

    @Override
    public int getCapacity() {
        return capacity;
    }

    @Override
    public boolean ordered() {
        return size() == 0 || !counterSortedList.isEmpty();
    }

    @Override
    public void toUnordered() {
        // Empty ineffective containers, including LinkedList
        counterSortedList = Lists.newLinkedList();
    }

    @Override
    public boolean isFull() {
        return size() >= capacity;
    }

    @Override
    public void offer(Counter<T> c) {
        put(c.item, c);
    }

    @Override
    public void offerToHead(Counter<T> c) {
        offer(c);
        counterSortedList.addFirst(c);
    }

    @Override
    public void retain(int newCapacity) {
        assert newCapacity > 0;

        this.capacity = newCapacity;
        if (size() > newCapacity) {
            sortAndRetain(newCapacity);
        }
    }

    @Override
    public void sort() {
        if (ordered()) {
            return;
        }
        counterSortedList = Lists.newLinkedList(counterMap.values());
        Collections.sort(counterSortedList, this.descending ? DESC_COMPARATOR : ASC_COMPARATOR);
    }

    @Override
    public void sortAndRetain() {
        sortAndRetain(capacity);
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
    public Counter<T> getLast() {
        return counterSortedList.isEmpty() ? null : counterSortedList.getLast();
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

    public CounterMap<T> copy() {
        CounterMap<T> result = new CounterMap<>(capacity, descending);
        result.counterMap = Maps.newHashMap(counterMap);
        return result;
    }

    /**
     * Sort and keep the expected size;
     */
    private void sortAndRetain(int newCapacity) {
        sort();
        retainSorted(newCapacity);
    }

    void retainSorted(int newCapacity) {
        this.capacity = newCapacity;
        if (this.size() > newCapacity) {
            Counter<T> toRemoved;
            for (int i = 0, n = this.size() - newCapacity; i < n; i++) {
                toRemoved = counterSortedList.pollLast();
                this.counterMap.remove(toRemoved.item);
            }
        }
    }
}
