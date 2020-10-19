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

import org.apache.kylin.measure.topn.BiTopNCounterSummary;
import org.apache.kylin.measure.topn.Counter;
import org.apache.kylin.measure.topn.ITopNCounter;
import org.apache.kylin.shaded.com.google.common.base.Preconditions;
import org.apache.kylin.shaded.com.google.common.collect.Maps;

import java.io.IOException;
import java.nio.ByteBuffer;

public class ExBiTopNCounter<T> extends BiTopNCounterSummary<ExItem<T>> {
    public static final int EXTRA_SPACE_RATE = 20;

    private ExBitmaps exBitmaps;

    public ExBiTopNCounter(int capacity, int nElems) {
        super(capacity);
        this.exBitmaps = new ExBitmaps(nElems);
    }

    public int getnElems() {
        return exBitmaps.getnElems();
    }

    @Override
    protected boolean occur(ExItem<T> item) {
        return exBitmaps.occur(item);
    }

    @Override
    public void offer(ExItem<T> item, double incrementCount) {
        super.offer(item, incrementCount);
        exBitmaps.offer(item);
    }

    @Override
    public ITopNCounter<ExItem<T>> merge(ITopNCounter<ExItem<T>> another0) {
        Preconditions.checkArgument(another0 instanceof ExBiTopNCounter,
                "The class for another is " + another0.getClass() + " which should be " + this.getClass());

        ExBiTopNCounter<T> another = (ExBiTopNCounter<T>) another0;
        if (another.isEmpty()) {
            return this;
        }

        if (!exBitmaps.intersects(another.exBitmaps)) {
            mergeDirectly(another);
        } else {
            // Find the estimation value for removed elements
            double b1 = getCounterSummaryBoundary();
            double b2 = another.getCounterSummaryBoundary();
            merge(another, b1, b2);
        }

        exBitmaps.or(another.exBitmaps);

        return this;
    }

    protected void mergeDirectly(ExBiTopNCounter<T> another) {
        toUnordered();

        for (Counter<ExItem<T>> entry : another.counterMap.values()) {
            Counter<ExItem<T>> counter = new Counter<>(entry.getItem(), entry.getCount());
            counterMap.put(counter.getItem(), counter);
        }

        if (counterMap.size() >= getRetainThresholdForMerge()) {
            retainUnsorted(capacity);
        }
    }

    @Override
    public String toString() {
        return super.toString() + ";RoaringBitmapCounters[" + exBitmaps.getCardinality() + "]";
    }

    /**
     * For TopNAggregator to avoid concurrency issues
     *
     * @return
     */
    public ExBiTopNCounter<T> copy() {
        ExBiTopNCounter<T> result = new ExBiTopNCounter<>(capacity, getnElems());
        result.counterMap = Maps.newHashMap(counterMap);
        result.exBitmaps = exBitmaps.copy();
        return result;
    }

    /**
     * Serialize this counter. The current counter is not modified.
     */
    public void writeFields(ByteBuffer out) throws IOException {
        exBitmaps.writeFields(out);
    }

    /**
     * Deserialize a counter from its serialized form.
     * <p> After deserialize, any changes to `in` should not affect the returned counter.
     */
    public void readFields(ByteBuffer in) throws IOException {
        exBitmaps.readFields(in);
    }
}
