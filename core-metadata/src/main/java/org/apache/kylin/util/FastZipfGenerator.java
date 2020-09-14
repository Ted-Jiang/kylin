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

package org.apache.kylin.util;

import java.util.NavigableMap;
import java.util.Random;
import java.util.TreeMap;

public class FastZipfGenerator {
    private Random random = new Random(0);
    private NavigableMap<Double, Integer> map;

    public FastZipfGenerator(int size, double skew) {
        map = computeMap(size, skew);
    }

    private static NavigableMap<Double, Integer> computeMap(int size, double skew) {
        NavigableMap<Double, Integer> map = new TreeMap<Double, Integer>();

        double div = 0;
        for (int i = 1; i <= size; i++) {
            div += (1 / Math.pow(i, skew));
        }

        double sum = 0;
        for (int i = 1; i <= size; i++) {
            double p = (1.0d / Math.pow(i, skew)) / div;
            sum += p;
            map.put(sum, i - 1);
        }
        return map;
    }

    public int next() {
        double value = random.nextDouble();
        return map.ceilingEntry(value).getValue() + 1;
    }
}
