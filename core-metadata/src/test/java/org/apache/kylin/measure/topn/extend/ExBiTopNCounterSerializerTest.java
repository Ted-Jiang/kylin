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
import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.metadata.datatype.DataType;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class ExBiTopNCounterSerializerTest extends LocalFileMetadataTestCase {

    private static final int nElems = 1;
    private static ExBiTopNCounterSerializer serializer;

    @BeforeClass
    public static void setUp() throws Exception {
        staticCreateTestMetadata();

        DataType.register("ex_bi_topn");
        serializer = new ExBiTopNCounterSerializer(DataType.getType("ex_bi_topn(10)"));
    }

    @AfterClass
    public static void after() throws Exception {
        cleanAfterClass();
    }

    @Test
    public void testSerialization() {
        Random rand = new Random();

        ExBiTopNCounter<ByteArray> vs = new ExBiTopNCounter<>(50, nElems);
        List<Integer[]> streams = new ArrayList<>(2);
        Integer[] stream1 = {};
        streams.add(stream1);
        Integer[] stream2 = {1, 1, 2, 9, 1, 2, 3, 7, 7, 1, 3, 1, 3, 4, 1, 2, 9, 1, 2, 3, 7, 5, 7, 3, 1, 1};
        streams.add(stream2);
        int keyLength = 4;
        for (Integer[] stream : streams) {
            byte[] keyArray = new byte[stream.length * keyLength];
            int offset = 0;
            for (Integer i : stream) {
                ByteArray[] elems = {new ByteArray(keyArray, offset, keyLength)};
                BytesUtil.writeUnsigned(i, keyArray, offset, keyLength);
                vs.offer(new ExItem.ExByteArrayItem(elems), rand.nextBoolean() ? 1.0 : -1.0);
                offset += keyLength;
            }
            ByteBuffer out = ByteBuffer.allocate(1024);
            serializer.serialize(vs, out);

            byte[] copyBytes = new byte[out.position()];
            System.arraycopy(out.array(), 0, copyBytes, 0, out.position());

            ByteBuffer in = ByteBuffer.wrap(copyBytes);
            ExBiTopNCounter vsNew = serializer.deserialize(in);

            Assert.assertEquals(vs.toString(), vsNew.toString());
        }
    }
}
