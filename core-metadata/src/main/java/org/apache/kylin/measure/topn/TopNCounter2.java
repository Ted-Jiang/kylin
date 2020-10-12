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

import java.util.PriorityQueue;

/**
 * Use PriorityQueue & LinkedList for element pruning
 *
 * @param <T> type of data in the stream to be summarized
 */
public class TopNCounter2<T> extends TopNCounter<T> {

    /**
     * @param capacity maximum size (larger capacities improve accuracy)
     */
    public TopNCounter2(int capacity) {
        super(capacity);
    }

    @Override
    public void sortUnsorted(int newCapacity) {
        if (counterMap.ordered()) {
            return;
        }
        // ----- nln(k)+2k < nln(n) => 2k < n
        if (newCapacity * 2 < counterMap.size()) {
            sortByPriorityQueue(newCapacity);
        } else {
            super.sortUnsorted(newCapacity);
        }
    }

    /**
     * Sort by priority queue
     *
     * @param newCapacity
     */
    private void sortByPriorityQueue(int newCapacity) {
        if (counterMap.ordered()) {
            return;
        }

        // Construct min-heap
        PriorityQueue<Counter<T>> counterQueue = new PriorityQueue<>(counterMap.getCapacity(),
                counterMap.isDescending() ? ASC_COMPARATOR : DESC_COMPARATOR);
        for (Counter<T> entry : counterMap.values()) {
            if (counterQueue.size() < newCapacity) {
                counterQueue.offer(entry);
            } else {
                Counter<T> entryTop = counterQueue.peek();
                if (entryTop != null && entryTop.count < entry.count) {
                    counterQueue.poll();
                    counterQueue.offer(entry);
                }
            }
        }
        // Reconstruct Map & SortedList
        counterMap.clear();
        Counter<T> entryTop;
        while ((entryTop = counterQueue.poll()) != null) {
            counterMap.offerToHead(entryTop);
        }
    }

    @Override
    public TopNCounter2<T> copy() {
        TopNCounter2<T> result = new TopNCounter2<>(getCapacity());
        result.counterMap = counterMap.copy();
        return result;
    }
}
