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

import org.apache.kylin.common.util.ByteBufferOutputStream;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class ExBitmaps {
    private final int nElems;
    private ImmutableRoaringBitmap[] bitmaps;
    private MutableRoaringBitmap[] mBitmaps;

    public ExBitmaps(int nElems) {
        this.nElems = nElems;
        this.mBitmaps = new MutableRoaringBitmap[nElems];
        this.bitmaps = new ImmutableRoaringBitmap[nElems];
        for (int i = 0; i < nElems; i++) {
            this.mBitmaps[i] = new MutableRoaringBitmap();
            this.bitmaps[i] = this.mBitmaps[i];
        }
    }

    private ExBitmaps(ImmutableRoaringBitmap[] bitmaps) {
        this.nElems = bitmaps.length;
        this.bitmaps = bitmaps;
    }

    public int getnElems() {
        return nElems;
    }

    private MutableRoaringBitmap[] getMutableBitmaps() {
        if (!(bitmaps[0] instanceof MutableRoaringBitmap)) {
            // convert to mutable bitmap
            this.mBitmaps = new MutableRoaringBitmap[nElems];
            for (int i = 0; i < nElems; i++) {
                this.mBitmaps[i] = bitmaps[i].toMutableRoaringBitmap();
                this.bitmaps[i] = this.mBitmaps[i];
            }
        }
        return this.mBitmaps;
    }

    public <T> boolean occur(ExItem<T> item) {
        MutableRoaringBitmap[] mBitmaps = getMutableBitmaps();
        for (int i = 0; i < mBitmaps.length; i++) {
            if (!mBitmaps[i].contains(item.getIntElem(i))) {
                return false;
            }
        }
        return true;
    }

    public <T> void offer(ExItem<T> item) {
        MutableRoaringBitmap[] mBitmaps = getMutableBitmaps();
        for (int i = 0; i < mBitmaps.length; i++) {
            mBitmaps[i].add(item.getIntElem(i));
        }
    }

    boolean intersects(ExBitmaps another) {
        for (int i = 0; i < bitmaps.length; i++) {
            if (!ImmutableRoaringBitmap.intersects(bitmaps[i], another.bitmaps[i])) {
                return false;
            }
        }
        return true;
    }

    void or(ExBitmaps another) {
        MutableRoaringBitmap[] mBitmaps1 = getMutableBitmaps();
        MutableRoaringBitmap[] mBitmaps2 = another.getMutableBitmaps();
        for (int i = 0; i < mBitmaps1.length; i++) {
            mBitmaps1[i].or(mBitmaps2[i]);
        }
    }

    List<Integer> getCardinality() {
        List<Integer> ret = new ArrayList<>(bitmaps.length);
        for (ImmutableRoaringBitmap bitmap : bitmaps) {
            ret.add(bitmap.getCardinality());
        }
        return ret;
    }

    void runOptimize() {
        if ((bitmaps[0] instanceof MutableRoaringBitmap)) {
            MutableRoaringBitmap[] mBitmaps = getMutableBitmaps();
            for (int i = 0; i < bitmaps.length; i++) {
                mBitmaps[i].runOptimize();
            }
        }
    }

    int serializedSizeInBytes() {
        int ret = 0;
        for (int i = 0; i < bitmaps.length; i++) {
            ret += bitmaps[i].serializedSizeInBytes();
        }
        return ret;
    }

    /**
     * Serialize this counter. The current counter is not modified.
     */
    public void writeFields(ByteBuffer out) throws IOException {
        runOptimize();

        if (out.remaining() < serializedSizeInBytes()) {
            throw new BufferOverflowException();
        }

        try (DataOutputStream dos = new DataOutputStream(new ByteBufferOutputStream(out))) {
            for (ImmutableRoaringBitmap bitmap : bitmaps) {
                bitmap.serialize(dos);
            }
        }
    }

    /**
     * Deserialize a counter from its serialized form.
     * <p> After deserialize, any changes to `in` should not affect the returned counter.
     */
    public void readFields(ByteBuffer in) throws IOException {
        this.bitmaps = new ImmutableRoaringBitmap[nElems];
        for (int i = 0; i < nElems; i++) {
            // only look at the metadata of the bitmap, no deserialization happens
            ImmutableRoaringBitmap initBitmap = new ImmutableRoaringBitmap(in);
            // make a copy of the content to be safe
            byte[] dst = new byte[initBitmap.serializedSizeInBytes()];
            in.get(dst);

            // ImmutableRoaringBitmap only maps the buffer, thus faster than constructing a MutableRoaringBitmap.
            // we'll convert to MutableRoaringBitmap later when mutate is needed
            bitmaps[i] = new ImmutableRoaringBitmap(ByteBuffer.wrap(dst));
        }
    }

    ExBitmaps copy() {
        ImmutableRoaringBitmap[] ret = new ImmutableRoaringBitmap[bitmaps.length];
        for (int i = 0; i < bitmaps.length; i++) {
            ret[i] = bitmaps[i].clone();
        }
        return new ExBitmaps(ret);
    }
}
