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

import org.apache.kylin.shaded.com.google.common.collect.Maps;

public class BiTopNCounter<T> extends BiTopNCounterSummary<T> {

    public static final int EXTRA_SPACE_RATE = 25;

    public BiTopNCounter(int capacity) {
        super(capacity);
    }

    @Override
    protected boolean occur(T item) {
        return true;
    }

    @Override
    public BiTopNCounter<T> copy() {
        BiTopNCounter<T> result = new BiTopNCounter<>(capacity);
        result.counterMap = Maps.newHashMap(counterMap);
        return result;
    }
}
