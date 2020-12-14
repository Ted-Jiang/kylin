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

public class ExTopNCounterSerializerTest extends LocalFileMetadataTestCase {

    private static final int nElems = 1;
    private static ExTopNCounterSerializer serializer;

    @BeforeClass
    public static void setUp() throws Exception {
        staticCreateTestMetadata();

        DataType.register("ex_topn");
        serializer = new ExTopNCounterSerializer(DataType.getType("ex_topn(10)"));
    }

    @AfterClass
    public static void after() throws Exception {
        cleanAfterClass();
    }

    @Test
    public void testSerialization() {
        int keyLength = 4;

        List<Integer[]> streams = new ArrayList<>(3);
        Integer[] stream1 = {5};
        streams.add(stream1);
        Integer[] stream2 = {1, 1, 2, 9, 1, 2, 3, 7, 7, 1, 3, 1, 1};
        streams.add(stream2);
        Integer[] stream3 = {4, 6, 2, 9, 1, 2, 3, 8, 8, 1, 5, 2, 1};
        streams.add(stream3);

        for (Integer[] stream : streams) {
            ExTopNAggregator aggregator = new ExTopNAggregator();
            for (Integer i : stream) {
                byte[] keyArray = new byte[keyLength];
                ByteArray[] elems = {new ByteArray(keyArray, 0, keyLength)};
                BytesUtil.writeUnsigned(i, keyArray, 0, keyLength);

                ExTopNCounter<ByteArray> vs = new ExTopNCounter<>(100 * 50, true, nElems);
                vs.offer(new ExItem.ExByteArrayItem(elems));

                aggregator.aggregate(vs);
            }

            ExTopNCounter<ByteArray> vs = aggregator.getState();
            ByteBuffer out = ByteBuffer.allocate(1024);
            serializer.serialize(vs, out);

            byte[] copyBytes = new byte[out.position()];
            System.arraycopy(out.array(), 0, copyBytes, 0, out.position());
            ByteBuffer in = ByteBuffer.wrap(copyBytes);

            ExTopNCounter<ByteArray> vsNew = serializer.deserialize(in);
            Assert.assertEquals(vs.toString(), vsNew.toString());
        }
    }
}