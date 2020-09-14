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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.shaded.com.google.common.collect.Lists;
import org.apache.kylin.shaded.com.google.common.collect.Maps;
import org.apache.kylin.util.FastZipfGenerator;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

@Ignore("For collecting accuracy statistics, not for functional test")
public class TopNCounterTest {

    protected static final int BATCH_SIZE = 10000;

    protected static int TOP_K;

    protected static int KEY_SPACE;

    protected static int TOTAL_RECORDS;

    protected static int SPACE_SAVING_ROOM;

    protected static int PARALLEL = 10;

    protected static boolean verbose = false;

    protected String dataFilePath;

    protected TopNCounterTest.HashMapConsumer accurateCounter;

    public TopNCounterTest() {
        TOP_K = 100;
        KEY_SPACE = 100 * TOP_K;
        TOTAL_RECORDS = 1000 * BATCH_SIZE; // 10 million
        SPACE_SAVING_ROOM = 100;
    }

    @Before
    public void setup() throws IOException {
        dataFilePath = prepareTestDate();

        accurateCounter = new TopNCounterTest.HashMapConsumer();
        feedDataToConsumer(dataFilePath, accurateCounter, 0, TOTAL_RECORDS);
    }

    @After
    public void cleanup() throws IOException {
        FileUtils.forceDelete(new File(dataFilePath));
    }

    private void outputMsg(String msg) {
        if (verbose)
            System.out.println(msg);
    }

    private String prepareTestDate() throws IOException {
        String[] allKeys = new String[KEY_SPACE];

        for (int i = 0; i < KEY_SPACE; i++) {
            allKeys[i] = RandomStringUtils.randomAlphabetic(20);
        }

        System.out.println("Start to create test random data...");
        long startTime = System.currentTimeMillis();
        FastZipfGenerator zipf = new FastZipfGenerator(KEY_SPACE, 0.5);

        File tempFile = File.createTempFile("ZipfDistribution", ".txt");

        if (tempFile.exists())
            FileUtils.forceDelete(tempFile);
        try (Writer fw = new OutputStreamWriter(new FileOutputStream(tempFile), StandardCharsets.UTF_8)) {
            for (int i = 0; i < TOTAL_RECORDS / BATCH_SIZE; i++) {
                StringBuilder sb = new StringBuilder();
                for (int j = 0; j < BATCH_SIZE; j++) {
                    int keyIndex = zipf.next() - 1;
                    sb.append(allKeys[keyIndex]).append('\n');
                }
                fw.write(sb.toString());
            }
        }

        System.out.println("Create test data takes : " + (System.currentTimeMillis() - startTime) + " milliseconds.");
        System.out.println("Test data in : " + tempFile.getAbsolutePath());

        return tempFile.getAbsolutePath();
    }

    @Ignore
    @Test
    public void testSingleSpaceSaving() throws IOException {
        TopNCounterTest.SpaceSavingConsumer spaceSavingCounter = new TopNCounterTest.SpaceSavingConsumer(
                TOP_K * SPACE_SAVING_ROOM);
        feedDataToConsumer(dataFilePath, spaceSavingCounter, 0, TOTAL_RECORDS);

        compareResult(spaceSavingCounter, accurateCounter);
    }

    @Test
    public void testParallelSpaceSaving() throws IOException, ClassNotFoundException {
        TopNCounterTest.SpaceSavingConsumer[] parallelCounters = new TopNCounterTest.SpaceSavingConsumer[PARALLEL];

        for (int i = 0; i < PARALLEL; i++) {
            parallelCounters[i] = new TopNCounterTest.SpaceSavingConsumer(TOP_K * SPACE_SAVING_ROOM);
        }

        int slice = TOTAL_RECORDS / PARALLEL;
        int startPosition = 0;
        for (int i = 0; i < PARALLEL; i++) {
            feedDataToConsumer(dataFilePath, parallelCounters[i], startPosition, startPosition + slice);
            startPosition += slice;
        }

        TopNCounterTest.SpaceSavingConsumer[] mergedCounters = singleMerge(parallelCounters);

        compareResult(mergedCounters[0], accurateCounter);
    }

    private void compareResult(TopNCounterTest.TestDataConsumer firstConsumer,
            TopNCounterTest.TestDataConsumer secondConsumer) {
        List<Pair<String, Double>> topResult1 = firstConsumer.getTopN(TOP_K);
        System.out.println("Get topN, Space saving takes " + firstConsumer.getSpentTime() + " milliseconds");
        List<Pair<String, Double>> realSequence = secondConsumer.getTopN(TOP_K);
        System.out.println("Get topN, Merge sort takes " + secondConsumer.getSpentTime() + " milliseconds");

        int error = 0;
        for (int i = 0; i < topResult1.size(); i++) {
            outputMsg("Compare " + i);

            if (isClose(topResult1.get(i).getSecond().doubleValue(), realSequence.get(i).getSecond().doubleValue())) {
                //            if (topResult1.get(i).getFirst().equals(realSequence.get(i).getFirst()) && topResult1.get(i).getSecond().doubleValue() == realSequence.get(i).getSecond().doubleValue()) {
                outputMsg("Passed; key:" + topResult1.get(i).getFirst() + ", value:" + topResult1.get(i).getSecond());
            } else {
                outputMsg("Failed; space saving key:" + topResult1.get(i).getFirst() + ", value:"
                        + topResult1.get(i).getSecond());
                outputMsg("Failed; correct key:" + realSequence.get(i).getFirst() + ", value:"
                        + realSequence.get(i).getSecond());
                error++;
            }
        }

        org.junit.Assert.assertEquals(0, error);
    }

    private boolean isClose(double value1, double value2) {
        return Math.abs(value1 - value2) < 5.0;
    }

    private TopNCounterTest.SpaceSavingConsumer[] singleMerge(TopNCounterTest.SpaceSavingConsumer[] consumers)
            throws IOException, ClassNotFoundException {
        List<TopNCounterTest.SpaceSavingConsumer> list = Lists.newArrayList();
        if (consumers.length == 1)
            return consumers;

        TopNCounterTest.SpaceSavingConsumer merged = new TopNCounterTest.SpaceSavingConsumer(TOP_K * SPACE_SAVING_ROOM);

        for (int i = 0, n = consumers.length; i < n; i++) {
            merged.merge(consumers[i]);
        }

        merged.retain(TOP_K * SPACE_SAVING_ROOM); // remove extra elements;
        return new TopNCounterTest.SpaceSavingConsumer[] { merged };

    }

    private TopNCounterTest.SpaceSavingConsumer[] binaryMerge(TopNCounterTest.SpaceSavingConsumer[] consumers)
            throws IOException, ClassNotFoundException {
        List<TopNCounterTest.SpaceSavingConsumer> list = Lists.newArrayList();
        if (consumers.length == 1)
            return consumers;

        for (int i = 0, n = consumers.length; i < n; i = i + 2) {
            if (i + 1 < n) {
                consumers[i].merge(consumers[i + 1]);
            }

            list.add(consumers[i]);
        }

        return binaryMerge(list.toArray(new TopNCounterTest.SpaceSavingConsumer[list.size()]));
    }

    private void feedDataToConsumer(String dataFile, TopNCounterTest.TestDataConsumer consumer, int startLine,
            int endLine) throws IOException {
        long startTime = System.currentTimeMillis();

        try (BufferedReader bufferedReader = new BufferedReader(
                new InputStreamReader(new FileInputStream(dataFile), StandardCharsets.UTF_8))) {
            int lineNum = 0;
            String line = bufferedReader.readLine();
            while (line != null) {
                if (lineNum >= startLine && lineNum < endLine) {
                    consumer.addElement(line, 1.0);
                }
                line = bufferedReader.readLine();
                lineNum++;
            }

            consumer.finishFeed();
        }

        System.out.println("feed data to " + consumer.getClass().getCanonicalName() + " take time (milliseconds): "
                + (System.currentTimeMillis() - startTime));
    }

    private static interface TestDataConsumer {
        public void addElement(String elementKey, double value);

        public List<Pair<String, Double>> getTopN(int k);

        public long getSpentTime();

        public void finishFeed();
    }

    private class SpaceSavingConsumer implements TopNCounterTest.TestDataConsumer {
        private long timeSpent = 0;
        private final int capacity;
        protected TopNCounter<String> vs;

        public SpaceSavingConsumer(int space) {
            this.capacity = space;
            this.vs = new TopNCounter<>(space);
        }

        public void addElement(String key, double value) {
            //outputMsg("Adding " + key + ":" + incrementCount);
            long startTime = System.currentTimeMillis();
            vs.offer(key, value);
            timeSpent += (System.currentTimeMillis() - startTime);
        }

        public void merge(SpaceSavingConsumer another) {
            long startTime = System.currentTimeMillis();
            vs.merge(another.vs);
            timeSpent += (System.currentTimeMillis() - startTime);
        }

        public void retain(int capacity) {
            long startTime = System.currentTimeMillis();
            vs.retain(capacity);
            timeSpent += (System.currentTimeMillis() - startTime);
        }

        public void finishFeed() {
            retain(capacity);
        }

        @Override
        public List<Pair<String, Double>> getTopN(int k) {
            long startTime = System.currentTimeMillis();
            List<Counter<String>> tops = vs.topK(k);
            List<Pair<String, Double>> allRecords = Lists.newArrayList();

            for (Counter<String> counter : tops)
                allRecords.add(Pair.newPair(counter.getItem(), counter.getCount()));
            timeSpent += (System.currentTimeMillis() - startTime);
            return allRecords;
        }

        @Override
        public long getSpentTime() {
            return timeSpent;
        }
    }

    private class HashMapConsumer implements TopNCounterTest.TestDataConsumer {

        private long timeSpent = 0;
        private Map<String, Double> hashMap;

        public HashMapConsumer() {
            hashMap = Maps.newHashMap();
        }

        public void addElement(String key, double value) {
            long startTime = System.currentTimeMillis();
            if (hashMap.containsKey(key)) {
                hashMap.put(key, hashMap.get(key) + value);
            } else {
                hashMap.put(key, value);
            }
            timeSpent += (System.currentTimeMillis() - startTime);
        }

        @Override
        public List<Pair<String, Double>> getTopN(int k) {
            long startTime = System.currentTimeMillis();
            List<Pair<String, Double>> allRecords = Lists.newArrayList();

            for (Map.Entry<String, Double> entry : hashMap.entrySet()) {
                allRecords.add(Pair.newPair(entry.getKey(), entry.getValue()));
            }

            Collections.sort(allRecords, new Comparator<Pair<String, Double>>() {
                @Override
                public int compare(Pair<String, Double> o1, Pair<String, Double> o2) {
                    return o1.getSecond() < o2.getSecond() ? 1 : (o1.getSecond() > o2.getSecond() ? -1 : 0);
                }
            });
            timeSpent += (System.currentTimeMillis() - startTime);
            return allRecords.subList(0, k);
        }

        @Override
        public long getSpentTime() {
            return timeSpent;
        }

        @Override
        public void finishFeed() {
        }
    }

}
