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

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.shaded.com.google.common.collect.Lists;
import org.apache.kylin.util.FastZipfGenerator;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.List;

@Ignore("For collecting accuracy statistics, not for functional test")
public class TopNCounterTest {

    protected static int BATCH_SIZE;

    protected static int TOP_K;

    protected static int KEY_SPACE;

    protected static int TOTAL_RECORDS;

    protected static int SPACE_SAVING_ROOM;

    protected static int PARALLEL = 100;

    protected static boolean verbose = false;

    protected static double precision = 5;

    protected String dataFilePath;

    public TopNCounterTest() {
        TOP_K = 100;
        KEY_SPACE = 100 * TOP_K;
        TOTAL_RECORDS = 100 * KEY_SPACE;
        SPACE_SAVING_ROOM = TopNCounter.EXTRA_SPACE_RATE;
    }

    @Before
    public void setup() throws IOException {
        dataFilePath = prepareTestDate();
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
        BATCH_SIZE = TOTAL_RECORDS / PARALLEL;

        String[] allKeys = new String[KEY_SPACE];

        for (int i = 0; i < KEY_SPACE; i++) {
            allKeys[i] = RandomStringUtils.randomAlphabetic(10);
        }

        System.out.println("Start to create test random data...");
        long startTime = System.currentTimeMillis();
        FastZipfGenerator zipf = new FastZipfGenerator(KEY_SPACE, 0.5);

        File tempFile = File.createTempFile("ZipfDistribution", ".txt");

        if (tempFile.exists())
            FileUtils.forceDelete(tempFile);
        try (Writer fw = new OutputStreamWriter(new FileOutputStream(tempFile), StandardCharsets.UTF_8)) {
            for (int i = 0; i < PARALLEL; i++) {
                StringBuilder sb = new StringBuilder();
                for (int j = 0; j < BATCH_SIZE; j++) {
                    int keyIndex = zipf.next() - 1;
                    sb.append(allKeys[keyIndex]).append(',');
                }
                String line = sb.toString();
                fw.write(line.substring(0, line.length() - 1));
                fw.write('\n');
            }
        }

        System.out.println("Create test data takes : " + (System.currentTimeMillis() - startTime) + " milliseconds.");
        System.out.println("Test data in : " + tempFile.getAbsolutePath());

        return tempFile.getAbsolutePath();
    }

    @Ignore
    @Test
    public void testSingleSpaceSaving() throws IOException {
        int capacity = TOP_K * SPACE_SAVING_ROOM;

        TopNCounterTest.TestDataConsumer accurateCounter = feedDataToConsumer(dataFilePath, TopNCounterEnum.Accurate,
                capacity);
        List<Pair<String, Double>> retAccurate = accurateCounter.getTopN(TOP_K);

        TopNCounterTest.TestDataConsumer spaceSavingCounter = feedDataToConsumer(dataFilePath, TopNCounterEnum.Current,
                capacity);
        List<Pair<String, Double>> retCurrent = spaceSavingCounter.getTopN(TOP_K);

        compareResult(retCurrent, retAccurate);
    }

    @Ignore
    @Test
    public void testCompareSingleSpaceSaving() throws IOException, ClassNotFoundException {
        int capacity = TOP_K * SPACE_SAVING_ROOM;

        System.out.println("===============Result for testCompareSingleSpaceSaving===============");
        TopNCounterTest.TestDataConsumer accurateCounter = feedDataToConsumer(dataFilePath, TopNCounterEnum.Accurate,
                capacity);
        List<Pair<String, Double>> retAccurate = accurateCounter.getTopN(TOP_K);
        System.out.println("Get topN,       Merge sort takes [data feeding] & [querying topK]: "
                + accurateCounter.getFeedSpentTime() + " & " + accurateCounter.getQuerySpentTime() + "ms");

        TopNCounterTest.TestDataConsumer spaceSavingCounterOld = feedDataToConsumer(dataFilePath, TopNCounterEnum.Old,
                capacity);
        List<Pair<String, Double>> retOld = spaceSavingCounterOld.getTopN(TOP_K);
        compareResult(retOld, retAccurate);
        System.out.println("Get topN,   TopNCounterOld takes [data feeding] & [querying topK]: "
                + spaceSavingCounterOld.getFeedSpentTime() + " & " + spaceSavingCounterOld.getQuerySpentTime() + "ms");

        TopNCounterTest.TestDataConsumer spaceSavingCounterOld2 = feedDataToConsumer(dataFilePath, TopNCounterEnum.Old2,
                capacity);
        List<Pair<String, Double>> retOld2 = spaceSavingCounterOld2.getTopN(TOP_K);
        compareResult(retOld2, retAccurate);
        System.out.println("Get topN,     TopNCounter2 takes [data feeding] & [querying topK]: "
                + spaceSavingCounterOld2.getFeedSpentTime() + " & " + spaceSavingCounterOld2.getQuerySpentTime()
                + "ms");

        TopNCounterTest.TestDataConsumer spaceSavingCounter = feedDataToConsumer(dataFilePath, TopNCounterEnum.Current,
                capacity);
        List<Pair<String, Double>> retCurrent = spaceSavingCounter.getTopN(TOP_K);
        compareResult(retCurrent, retAccurate);
        System.out.println("Get topN,      TopNCounter takes [data feeding] & [querying topK]: "
                + spaceSavingCounter.getFeedSpentTime() + " & " + spaceSavingCounter.getQuerySpentTime() + "ms");
    }

    @Test
    public void testParallelSpaceSaving() throws IOException, ClassNotFoundException {
        TopNCounterTest.TestDataConsumer[] mergedCountersAccurate = getMergedCounters(TopNCounterEnum.Accurate);
        List<Pair<String, Double>> retAccurate = mergedCountersAccurate[0].getTopN(TOP_K);

        TopNCounterTest.TestDataConsumer[] mergedCounters = getMergedCounters(TopNCounterEnum.Current);
        List<Pair<String, Double>> retCurrent = mergedCounters[0].getTopN(TOP_K);

        compareResult(retCurrent, retAccurate);
    }

    @Ignore
    @Test
    public void testCompareParallelSpaceSaving() throws IOException, ClassNotFoundException {
        System.out.println("===============Result for testCompareParallelSpaceSaving===============");
        TopNCounterTest.TestDataConsumer[] mergedCountersAccurate = getMergedCounters(TopNCounterEnum.Accurate);
        List<Pair<String, Double>> retAccurate = mergedCountersAccurate[0].getTopN(TOP_K);
        System.out.println("Get topN,       Merge sort takes [data feeding] & [querying topK]: "
                + mergedCountersAccurate[0].getFeedSpentTime() + " & " + mergedCountersAccurate[0].getQuerySpentTime()
                + "ms");

        TopNCounterTest.TestDataConsumer[] mergedCountersOld = getMergedCounters(TopNCounterEnum.Old);
        List<Pair<String, Double>> retOld = mergedCountersOld[0].getTopN(TOP_K);
        compareResult(retOld, retAccurate);
        System.out.println("Get topN,   TopNCounterOld takes [data feeding] & [querying topK]: "
                + mergedCountersOld[0].getFeedSpentTime() + " & " + mergedCountersOld[0].getQuerySpentTime() + "ms");

        TopNCounterTest.TestDataConsumer[] mergedCountersOld2 = getMergedCounters(TopNCounterEnum.Old2);
        List<Pair<String, Double>> retOld2 = mergedCountersOld2[0].getTopN(TOP_K);
        compareResult(retOld2, retAccurate);
        System.out.println("Get topN,     TopNCounter2 takes [data feeding] & [querying topK]: "
                + mergedCountersOld2[0].getFeedSpentTime() + " & " + mergedCountersOld2[0].getQuerySpentTime() + "ms");

        TopNCounterTest.TestDataConsumer[] mergedCounters = getMergedCounters(TopNCounterEnum.Current);
        List<Pair<String, Double>> retCurrent = mergedCounters[0].getTopN(TOP_K);
        compareResult(retCurrent, retAccurate);
        System.out.println("Get topN,      TopNCounter takes [data feeding] & [querying topK]: "
                + mergedCounters[0].getFeedSpentTime() + " & " + mergedCounters[0].getQuerySpentTime() + "ms");
    }

    private void compareResult(List<Pair<String, Double>> topResult1, List<Pair<String, Double>> realSequence) {
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

        System.out.println("Error number " + error);
        org.junit.Assert.assertEquals(0, error);
    }

    private boolean isClose(double value1, double value2) {
        return Math.abs(value1 - value2) <= precision;
    }

    private TopNCounterTest.TestDataConsumer[] getMergedCounters(TopNCounterEnum type)
            throws IOException, ClassNotFoundException {
        int capacity = TOP_K * SPACE_SAVING_ROOM;

        TopNCounterTest.TestDataConsumer[] parallelCounters = feedDataToConsumer(dataFilePath, type, capacity,
                PARALLEL);

        TopNCounterTest.TestDataConsumer[] mergedCounters = singleMerge(parallelCounters, type);
        return mergedCounters;
    }

    private TopNCounterTest.TestDataConsumer[] singleMerge(TopNCounterTest.TestDataConsumer[] consumers,
                                                           TopNCounterEnum type) throws IOException, ClassNotFoundException {
        if (consumers.length == 1)
            return consumers;

        TopNCounterTest.TestDataConsumer merged = createTestDataConsumer(type, TOP_K * SPACE_SAVING_ROOM);

        for (int i = 0, n = consumers.length; i < n; i++) {
            merged.merge(consumers[i]);
        }

        merged.retain(TOP_K * SPACE_SAVING_ROOM); // remove extra elements;
        return new TopNCounterTest.TestDataConsumer[]{merged};

    }

    private TopNCounterTest.TestDataConsumer[] binaryMerge(TopNCounterTest.TestDataConsumer[] consumers)
            throws IOException, ClassNotFoundException {
        List<TopNCounterTest.TestDataConsumer> list = Lists.newArrayList();
        if (consumers.length == 1)
            return consumers;

        for (int i = 0, n = consumers.length; i < n; i = i + 2) {
            if (i + 1 < n) {
                consumers[i].merge(consumers[i + 1]);
            }

            list.add(consumers[i]);
        }

        return binaryMerge(list.toArray(new TopNCounterTest.TestDataConsumer[list.size()]));
    }

    private TopNCounterTest.TestDataConsumer feedDataToConsumer(String dataFile, TopNCounterEnum type, int capacity)
            throws IOException {
        TopNCounterTest.TestDataConsumer[] ret = feedDataToConsumer(dataFile, type, capacity, 1);
        return ret[0];
    }

    private TopNCounterTest.TestDataConsumer[] feedDataToConsumer(String dataFile, TopNCounterEnum type, int capacity,
                                                                  int n) throws IOException {
        TopNCounterTest.TestDataConsumer[] ret = new TopNCounterTest.TestDataConsumer[n];

        for (int i = 0; i < n; i++) {
            ret[i] = createTestDataConsumer(type, capacity);
        }

        try (BufferedReader bufferedReader = new BufferedReader(
                new InputStreamReader(new FileInputStream(dataFile), StandardCharsets.UTF_8))) {
            for (int i = 0; i < n; i++) {
                long startTime = System.currentTimeMillis();

                String line = bufferedReader.readLine();
                if (line == null) {
                    System.out.println("Finished feeding data to the " + i + "th Consumer");
                    break;
                }
                for (String element : line.split(",")) {
                    ret[i].addElement(element, 1.0);
                }
                ret[i].finishFeed();

                outputMsg("feed data to " + i + "th Consumer with type " + type + " take time (milliseconds): "
                        + (System.currentTimeMillis() - startTime));
            }
        }

        return ret;
    }

    private static abstract class TestDataConsumer {
        private ITopNCounter<String> vs;
        protected long timeSpentFeed = 0L;
        protected long timeSpentQuery = 0L;

        public TestDataConsumer(ITopNCounter<String> vs) {
            this.vs = vs;
        }

        public abstract void retain(int capacity);

        public abstract void finishFeed();

        public void addElement(String key, double value) {
            //outputMsg("Adding " + key + ":" + incrementCount);
            long startTime = System.currentTimeMillis();
            vs.offer(key, value);
            timeSpentFeed += (System.currentTimeMillis() - startTime);
        }

        public void merge(TestDataConsumer another) {
            long startTime = System.currentTimeMillis();
            vs.merge(another.vs);
            timeSpentQuery += (System.currentTimeMillis() - startTime);
            timeSpentQuery += another.timeSpentQuery;

            timeSpentFeed += another.timeSpentFeed;
        }

        public List<Pair<String, Double>> getTopN(int k) {
            long startTime = System.currentTimeMillis();
            List<Counter<String>> tops = vs.topK(k);
            List<Pair<String, Double>> allRecords = Lists.newArrayList();

            for (Counter<String> counter : tops)
                allRecords.add(Pair.newPair(counter.getItem(), counter.getCount()));
            timeSpentQuery += (System.currentTimeMillis() - startTime);
            return allRecords;
        }

        public long getFeedSpentTime() {
            return timeSpentFeed;
        }

        public long getQuerySpentTime() {
            return timeSpentQuery;
        }
    }

    private static class SpaceSavingConsumer extends TopNCounterTest.TestDataConsumer {

        protected TopNCounterSummaryBase<String> vs;

        public SpaceSavingConsumer(TopNCounterEnum type, int capacity) {
            this((TopNCounterSummaryBase<String>) createTopNCounter(type, capacity));
        }

        public SpaceSavingConsumer(TopNCounterSummaryBase<String> vs) {
            super(vs);
            this.vs = vs;
        }

        @Override
        public void retain(int capacity) {
            long startTime = System.currentTimeMillis();
            vs.retain(capacity);
            timeSpentQuery += (System.currentTimeMillis() - startTime);
        }

        @Override
        public void finishFeed() {
            long startTime = System.currentTimeMillis();
            vs.sortAndRetain();
            timeSpentFeed += (System.currentTimeMillis() - startTime);
        }
    }

    private static class HashMapConsumer extends TopNCounterTest.TestDataConsumer {

        protected TopNPreciseCounter<String> vs;

        public HashMapConsumer() {
            this((TopNPreciseCounter<String>) createTopNCounter(TopNCounterEnum.Accurate, Integer.MAX_VALUE));
        }

        public HashMapConsumer(TopNPreciseCounter<String> vs) {
            super(vs);
            this.vs = vs;
        }

        @Override
        public void retain(int capacity) {
            long startTime = System.currentTimeMillis();
            vs.sort();
            timeSpentQuery += (System.currentTimeMillis() - startTime);
        }

        @Override
        public void finishFeed() {
            long startTime = System.currentTimeMillis();
            vs.sort();
            timeSpentFeed += (System.currentTimeMillis() - startTime);
        }
    }

    private enum TopNCounterEnum {
        Current, Old2, Old, Accurate
    }

    private TestDataConsumer createTestDataConsumer(TopNCounterEnum type, int capacity) {
        switch (type) {
            case Accurate:
                return new HashMapConsumer();
            case Old:
            case Old2:
            case Current:
            default:
                return new SpaceSavingConsumer(type, capacity);
        }
    }

    private static ITopNCounter<String> createTopNCounter(TopNCounterEnum type, int capacity) {
        switch (type) {
            case Accurate:
                return new TopNPreciseCounter<>();
            case Old:
                return new TopNCounterOld<>(capacity);
            case Old2:
                return new TopNCounter2<>(capacity);
            case Current:
            default:
                return new TopNCounter<>(capacity);
        }
    }
}
