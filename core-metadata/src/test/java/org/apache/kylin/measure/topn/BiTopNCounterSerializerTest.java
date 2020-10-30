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

import com.google.common.base.Stopwatch;
import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.metadata.datatype.DataType;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class BiTopNCounterSerializerTest extends LocalFileMetadataTestCase {

    private static BiTopNCounterSerializer serializer;

    @BeforeClass
    public static void setUp() throws Exception {
        staticCreateTestMetadata();

        DataType.register("bi_topn");
        serializer = new BiTopNCounterSerializer(DataType.getType("bi_topn(10)"));
    }

    @AfterClass
    public static void after() throws Exception {
        cleanAfterClass();
    }

    @Test
    public void testSerialization() {
        Random rand = new Random();
        Stopwatch sw = new Stopwatch();
        long timeCost;

        int size = 50000;
        BiTopNCounter<ByteArray> vs = new BiTopNCounter<ByteArray>(size);
        for (int i = 0; i < size; i++) {
            vs.offer(new ByteArray(Bytes.toBytes(rand.nextInt(2500))), rand.nextBoolean() ? 1.0 : -1.0);
        }
        ByteBuffer out = ByteBuffer.allocate(20480);
        sw.start();
        serializer.serialize(vs, out);
        timeCost = sw.elapsed(TimeUnit.MILLISECONDS);
        System.out.println("TimeCost " + timeCost + "ms for serialization");

        byte[] copyBytes = new byte[out.position()];
        System.arraycopy(out.array(), 0, copyBytes, 0, out.position());

        ByteBuffer in = ByteBuffer.wrap(copyBytes);
        sw.reset();
        sw.start();
        BiTopNCounter<ByteArray> vsNew = serializer.deserialize(in);
        timeCost = sw.elapsed(TimeUnit.MILLISECONDS);
        System.out.println("TimeCost " + timeCost + "ms for deserialization");
        sw.stop();

        Assert.assertEquals(vs.toString(), vsNew.toString());

    }
}
