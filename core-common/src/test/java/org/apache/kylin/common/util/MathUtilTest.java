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

package org.apache.kylin.common.util;

import com.google.common.base.Stopwatch;
import org.apache.kylin.shaded.com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class MathUtilTest {

    @Test
    public void testFindKthElement() {
        int size = 100000;
        Comparator<Double> comparator = Comparator.comparingDouble(i -> -i);
        Random rand = new Random();
        List<Double> list = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            list.add(rand.nextDouble());
        }

        int k = size / 2;

        long timeCost;
        List<Double> list1 = Lists.newArrayList(list);
        Stopwatch sw = new Stopwatch();
        sw.start();
        Double kthElem1 = MathUtil.findKthElement(comparator, list1, k, null);
        timeCost = sw.elapsed(TimeUnit.MILLISECONDS);
        System.out.println("Time elapsed " + timeCost + "ms for findKthElement");

        List<Double> list2 = Lists.newArrayList(list);
        sw.reset();
        sw.start();
        Collections.sort(list2, comparator);
        Double kthElem2 = list2.get(k - 1);
        timeCost = sw.elapsed(TimeUnit.MILLISECONDS);
        System.out.println("Time elapsed " + timeCost + "ms for sort");
        sw.stop();

        Assert.assertEquals(kthElem1, kthElem2);
    }
}
