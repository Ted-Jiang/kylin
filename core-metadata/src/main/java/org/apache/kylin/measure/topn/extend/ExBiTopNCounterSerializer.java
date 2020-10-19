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
import org.apache.kylin.dimension.DictionaryDimEnc;
import org.apache.kylin.measure.topn.Counter;
import org.apache.kylin.measure.topn.DoubleDeltaSerializer;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.datatype.DataTypeSerializer;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;

public class ExBiTopNCounterSerializer extends DataTypeSerializer<ExBiTopNCounter<ByteArray>> {
    private DoubleDeltaSerializer dds = new DoubleDeltaSerializer(3);

    private static final int DEFAULT_MAX_SIZE = 1024;

    private int precision;

    private int scale;

    public ExBiTopNCounterSerializer(DataType dataType) {
        this.precision = dataType.getPrecision();
        this.scale = dataType.getScale();
        if (scale < 0) {
            scale = DictionaryDimEnc.MAX_ENCODING_LENGTH;
        }
    }

    @Override
    public int peekLength(ByteBuffer in) {
        int mark = in.position();

        @SuppressWarnings("unused")
        int capacity = in.getInt();
        int size = in.getInt();
        int nElems = in.getInt();

        int len = in.position() - mark;
        if (size != 0) {
            int keyLength = 0;
            for (int i = 0; i < nElems; i++) {
                keyLength += in.getInt();
            }
            @SuppressWarnings("unused")
            double valueBase = in.getDouble();
            dds.deserialize(in);
            int mark0 = in.position();
            // only look at the metadata of the bitmap, no deserialization happens
            for (int i = 0; i < nElems; i++) {
                ImmutableRoaringBitmap bitmap = new ImmutableRoaringBitmap(in);
                mark0 += bitmap.serializedSizeInBytes();
                in.position(mark0);
            }
            len = in.position() - mark;
            len += keyLength * size;
        }

        in.position(mark);
        return len;
    }

    @Override
    public int maxLength() {
        return topNMaxLength() + bitmapMaxLength();
    }

    private int topNMaxLength() {
        return Math.max(precision * ExBiTopNCounter.EXTRA_SPACE_RATE * storageBytesEstimatePerCounter(), 1024 * 1024); // use at least 1M
    }

    private int bitmapMaxLength() {
        // the bitmap is non-fixed length, and we just assume 8MB here, maybe change it later
        // some statistics for bitmap:
        // 1 million distinct keys takes about 2MB storage
        // 5 million takes 10MB
        // 10 million takes 12MB
        return 8 * 1024 * 1024;
    }

    @Override
    public void serialize(ExBiTopNCounter<ByteArray> value, ByteBuffer out) {
        int nElems = value.getnElems();

        // write out headers
        out.putInt(value.getCapacity());
        out.putInt(value.size());
        out.putInt(nElems);

        if (value.size() == 0) {
            return;
        }

        List<Counter<ExItem<ByteArray>>> peek = value.topK(1);
        ExItem<ByteArray> item = peek.get(0).getItem();
        for (int i = 0; i < nElems; i++) {
            out.putInt(item.elems[i].length());
        }

        // write out values
        double[] counters = value.getCounters();
        double valueBase = counters[0];
        for (int i = 0; i < counters.length; i++) {
            counters[i] -= valueBase;
        }
        out.putDouble(valueBase);
        dds.serialize(counters, out);

        // write out bitmaps
        try {
            value.writeFields(out);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        // write out keys
        Iterator<Counter<ExItem<ByteArray>>> iterator = value.iterator();
        while (iterator.hasNext()) {
            item = iterator.next().getItem();
            for (ByteArray elem : item.elems) {
                out.put(elem.array(), elem.offset(), elem.length());
            }
        }
    }

    @Override
    public ExBiTopNCounter<ByteArray> deserialize(ByteBuffer in) {
        // read headers
        int capacity = in.getInt();
        int size = in.getInt();
        int nElems = in.getInt();

        ExBiTopNCounter<ByteArray> counter = new ExBiTopNCounter<>(capacity, nElems);
        if (size == 0) {
            return counter;
        }

        int[] elemLens = new int[nElems];
        int keyLength = 0;
        for (int i = 0; i < nElems; i++) {
            elemLens[i] = in.getInt();
            keyLength += elemLens[i];
        }

        // read values
        double valueBase = in.getDouble();
        double[] counters = dds.deserialize(in);
        for (int i = 0; i < counters.length; i++) {
            counters[i] += valueBase;
        }

        // read bitmaps
        try {
            counter.readFields(in);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        // read keys
        byte[] keyArray = new byte[size * keyLength];
        int offset = 0;
        for (int i = 0; i < size; i++) {
            ByteArray[] byteArrays = new ByteArray[nElems];
            for (int j = 0; j < nElems; j++) {
                byteArrays[j] = new ByteArray(keyArray, offset, elemLens[j]);
                in.get(keyArray, offset, elemLens[j]);
                offset += elemLens[j];
            }
            counter.offerToHead(new ExItem.ExByteArrayItem(byteArrays), counters[i]);
        }

        return counter;
    }

    @Override
    public int getStorageBytesEstimate() {
        return getTopNStorageBytesEstimate() + getBitmapStorageBytesEstimate();
    }

    @Override
    protected double getStorageBytesEstimate(double averageNumOfElementsInCounter) {
        return getTopNStorageBytesEstimate(averageNumOfElementsInCounter) + getBitmapStorageBytesEstimate(averageNumOfElementsInCounter);
    }

    private int getTopNStorageBytesEstimate() {
        return precision * ExBiTopNCounter.EXTRA_SPACE_RATE * storageBytesEstimatePerCounter();
    }

    private int getBitmapStorageBytesEstimate() {
        // It's difficult to decide the size before data was ingested, comparing with HLLCounter(16) as 64KB, here is assumption
        return 8 * 1024;
    }

    private double getBitmapStorageBytesEstimate(double averageNumOfElementsInCounter) {
        // MappeableArrayContainer DEFAULT_MAX_SIZE = 4096
        if (averageNumOfElementsInCounter < DEFAULT_MAX_SIZE) {
            // 8 = 4 + 4 for SERIAL_COOKIE_NO_RUNCONTAINER + size
            // size * 8 = 2 * size + 2 * size + 4 * size as keys + values Cardinality + startOffsets
            // size * 8 for values array
            return 8 + averageNumOfElementsInCounter * 16;
        } else {
            return getBitmapStorageBytesEstimate();
        }
    }

    private double getTopNStorageBytesEstimate(double averageNumOfElementsInCounter) {
        if (averageNumOfElementsInCounter < precision * ExBiTopNCounter.EXTRA_SPACE_RATE) {
            return averageNumOfElementsInCounter * storageBytesEstimatePerCounter() + 12;
        } else {
            return getTopNStorageBytesEstimate();
        }
    }

    private int storageBytesEstimatePerCounter() {
        return (scale + 8);
    }
}
