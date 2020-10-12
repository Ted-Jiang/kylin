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

import java.util.Comparator;
import java.util.List;

public interface ITopNCounter<T> extends Iterable<Counter<T>>, java.io.Serializable {

    Comparator<Counter> ASC_COMPARATOR = Comparator.comparingDouble(Counter::getCount);

    Comparator<Counter> DESC_COMPARATOR = (Counter o1, Counter o2) -> Double.compare(o2.getCount(), o1.getCount());

    void offer(T item);

    void offer(T item, double incrementCount);

    ITopNCounter<T> merge(ITopNCounter<T> another);

    List<Counter<T>> topK(int k);

    /**
     * Get the counter values in ascending order which will be used for better serialization
     *
     * @return
     */
    double[] getCounters();

    int size();

    /**
     * For TopNAggregator to avoid concurrency issues
     *
     * @return
     */
    ITopNCounter<T> copy();
}
