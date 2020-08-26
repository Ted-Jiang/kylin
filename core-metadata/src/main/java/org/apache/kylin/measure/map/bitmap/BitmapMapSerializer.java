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

package org.apache.kylin.measure.map.bitmap;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.datatype.DataTypeSerializer;

public class BitmapMapSerializer extends DataTypeSerializer<BitmapCounterMap> {
    private static final BitmapCounterMapFactory factory = RoaringBitmapCounterMapFactory.INSTANCE;
    private static final BitmapCounterMap DELEGATE = factory.newBitmapMap();

    private static final int IS_RESULT_FLAG = -1;
    private static final int RESULT_SIZE = 9;

    // called by reflection
    public BitmapMapSerializer(DataType type) {
    }

    @Override
    public void serialize(BitmapCounterMap value, ByteBuffer out) {
        if (value == null) {
            value = factory.newBitmapMap();
        }
        try {
            value.write(out);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public BitmapCounterMap deserialize(ByteBuffer in) {
        try {
            if (peekLength(in) == RESULT_SIZE) {
                int flag = BytesUtil.readVInt(in);
                return factory.newBitmap(in.getLong());
            } else {
                return factory.newBitmapMap(in);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int peekLength(ByteBuffer in) {
        ByteBuffer buffer = in.slice();
        //The result of getInt will not be -1 if it's not trimmed
        int flag = BytesUtil.readVInt(buffer);
        if (flag == IS_RESULT_FLAG) {
            return RESULT_SIZE;
        } else {
            return DELEGATE.peekLength(in);
        }
    }

    @Override
    public int maxLength() {
        // the bitmap is non-fixed length, and we just assume 8MB here, maybe change it later
        // some statistics for bitmap:
        // 1 million distinct keys takes about 2MB storage
        // 5 million takes 10MB
        // 10 million takes 12MB
        return 8 * 1024 * 1024;
    }

    @Override
    public int getStorageBytesEstimate() {
        // It's difficult to decide the size before data was ingested, comparing with HLLCounter(16) as 64KB, here is assumption
        return 8 * 1024;
    }

    @Override
    public boolean supportDirectReturnResult() {
        return true;
    }

    @Override
    public ByteBuffer getFinalResult(ByteBuffer in) {
        ByteBuffer out = ByteBuffer.allocate(RESULT_SIZE);
        try {
            BitmapCounterMap counter = factory.newBitmapMap(in);
            BytesUtil.writeVInt(IS_RESULT_FLAG, out);
            out.putLong(counter.getCount());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        out.flip();
        return out;
    }
}