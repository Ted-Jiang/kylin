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

package org.apache.kylin.measure.topn.extend;

import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.measure.MeasureAggregator;

/**
 *
 */
@SuppressWarnings("serial")
public class ExTopNAggregator extends MeasureAggregator<ExTopNCounter<ByteArray>> {

    int capacity = 0;
    ExTopNCounter<ByteArray> sum = null;

    @Override
    public void reset() {
        sum = null;
    }

    @Override
    public void aggregate(ExTopNCounter<ByteArray> value) {
        if (sum == null) {
            capacity = value.getCapacity();
            boolean descending = value.isDescending();
            int nElems = value.getnElems();
            sum = new ExTopNCounter<>(capacity * 10, descending, nElems);
        }
        sum.merge(value);
    }

    @Override
    public ExTopNCounter<ByteArray> aggregate(ExTopNCounter<ByteArray> value1, ExTopNCounter<ByteArray> value2) {
        int thisCapacity = value1.getCapacity();
        boolean thisDescending = value1.isDescending();
        int thisNElems = value1.getnElems();
        ExTopNCounter<ByteArray> aggregated = new ExTopNCounter<>(thisCapacity * 2, thisDescending, thisNElems);
        aggregated.merge(value1);
        aggregated.merge(value2);
        aggregated.retain(thisCapacity);
        return aggregated;
    }

    @Override
    public ExTopNCounter<ByteArray> getState() {
        sum.retain(capacity);
        return sum;
    }

    @Override
    public int getMemBytesEstimate() {
        return 8 * capacity / 4;
    }

    public ExTopNAggregator copy() {
        ExTopNAggregator result = new ExTopNAggregator();
        result.capacity = this.capacity;
        ExTopNCounter<ByteArray> cpCounter = sum.copy();
        result.sum = cpCounter;
        return result;
    }

}
