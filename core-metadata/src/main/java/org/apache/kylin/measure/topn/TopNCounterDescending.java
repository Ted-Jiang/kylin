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

import java.util.Map;

public abstract class TopNCounterDescending<T> extends TopNCounterSummary<T> {

    public TopNCounterDescending(int capacity) {
        super(capacity);
    }

    protected abstract double getMinimum();

    protected double getEstimationOfRemoved() {
        return getMinimum();
    }

    protected ITopNCounter<T> merge(TopNCounterSummary<T> another, double e1, double e2) {
        // Empty ineffective containers, including LinkedList
        counterSortedList = Lists.newLinkedList();

        if (another.size() >= another.capacity) {
            for (Counter<T> entry : counterMap.values()) {
                entry.count += e2;
            }
        }

        for (Map.Entry<T, Counter<T>> entry : another.counterMap.entrySet()) {
            Counter<T> counter = counterMap.get(entry.getKey());
            if (counter != null) {
                counter.setCount(counter.getCount() + (entry.getValue().count - e2));
            } else {
                counter = new Counter<T>(entry.getValue().getItem(), entry.getValue().count + e1);
                this.counterMap.put(entry.getValue().getItem(), counter);
            }
        }

        retainUnsorted(capacity);
        return this;
    }
}
