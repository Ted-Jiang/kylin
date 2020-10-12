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

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.measure.topn.Counter;
import org.apache.kylin.measure.topn.ITopNCounter;
import org.apache.kylin.measure.topn.TopNCounterSummary;
import org.apache.kylin.measure.topn.TopNPreciseCounter;
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
import java.util.Random;

public class ExTopNCounterTest {
    private static final int nElems = 1;

    protected static int BATCH_SIZE;

    protected static int TOP_K;

    protected static int KEY_SPACE;

    protected static int TOTAL_RECORDS;

    protected static int SPACE_SAVING_ROOM;

    protected static int PARALLEL = 100;

    protected static boolean verbose = false;

    protected static double precision = 5;


    protected String dataFilePath;

    public ExTopNCounterTest() {
        TOP_K = 100;
        KEY_SPACE = 100 * TOP_K;
        TOTAL_RECORDS = 100 * KEY_SPACE;
        SPACE_SAVING_ROOM = TopNCounterSummary.EXTRA_SPACE_RATE;
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

        Integer[] allKeys = new Integer[KEY_SPACE];

        Random rand = new Random();
        for (int i = 0; i < KEY_SPACE; i++) {
            allKeys[i] = rand.nextInt();
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
    public void testCompareParallelSpaceSaving() throws IOException, ClassNotFoundException {
        System.out.println("===============Result for testCompareParallelSpaceSaving===============");
        TestDataConsumer[] mergedCountersAccurate = getMergedCounters(TopNCounterEnum.Accurate);
        List<Pair<ExItem<Integer>, Double>> retAccurate = mergedCountersAccurate[0].getTopN(TOP_K);
        System.out.println("Get topN,       Merge sort takes [data feeding] & [querying topK]: "
                + mergedCountersAccurate[0].getFeedSpentTime() + " & " + mergedCountersAccurate[0].getQuerySpentTime()
                + "ms");

        TestDataConsumer[] mergedCounters = getMergedCounters(TopNCounterEnum.Current);
        List<Pair<ExItem<Integer>, Double>> retCurrent = mergedCounters[0].getTopN(TOP_K);
        compareResult(retCurrent, retAccurate);
        System.out.println("Get topN,      TopNCounter takes [data feeding] & [querying topK]: "
                + mergedCounters[0].getFeedSpentTime() + " & " + mergedCounters[0].getQuerySpentTime() + "ms");
    }

    private void compareResult(List<Pair<ExItem<Integer>, Double>> topResult1, List<Pair<ExItem<Integer>, Double>> realSequence) {
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

    private TestDataConsumer[] getMergedCounters(TopNCounterEnum type)
            throws IOException, ClassNotFoundException {
        int capacity = TOP_K * SPACE_SAVING_ROOM;

        TestDataConsumer[] parallelCounters = feedDataToConsumer(dataFilePath, type, capacity,
                PARALLEL);

        TestDataConsumer[] mergedCounters = singleMerge(parallelCounters, type);
        return mergedCounters;
    }

    private TestDataConsumer[] singleMerge(TestDataConsumer[] consumers,
                                           TopNCounterEnum type) throws IOException, ClassNotFoundException {
        if (consumers.length == 1)
            return consumers;

        TestDataConsumer merged = createTestDataConsumer(type, TOP_K * SPACE_SAVING_ROOM);

        for (int i = 0, n = consumers.length; i < n; i++) {
            merged.merge(consumers[i]);
        }

        merged.retain(TOP_K * SPACE_SAVING_ROOM); // remove extra elements;
        return new TestDataConsumer[]{merged};

    }

    private TestDataConsumer feedDataToConsumer(String dataFile, TopNCounterEnum type, int capacity)
            throws IOException {
        TestDataConsumer[] ret = feedDataToConsumer(dataFile, type, capacity, 1);
        return ret[0];
    }

    private TestDataConsumer[] feedDataToConsumer(String dataFile, TopNCounterEnum type, int capacity,
                                                  int n) throws IOException {
        TestDataConsumer[] ret = new TestDataConsumer[n];

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
                    ret[i].addElement(Integer.parseInt(element), -1.0);
                }
                ret[i].finishFeed();

                outputMsg("feed data to " + i + "th Consumer with type " + type + " take time (milliseconds): "
                        + (System.currentTimeMillis() - startTime));
            }
        }

        return ret;
    }

    private static abstract class TestDataConsumer {
        private ITopNCounter<ExItem<Integer>> vs;
        protected long timeSpentFeed = 0L;
        protected long timeSpentQuery = 0L;

        public TestDataConsumer(ITopNCounter<ExItem<Integer>> vs) {
            this.vs = vs;
        }

        public abstract void retain(int capacity);

        public abstract void finishFeed();

        public void addElement(Integer key, double value) {
            //outputMsg("Adding " + key + ":" + incrementCount);
            long startTime = System.currentTimeMillis();
            Integer[] elems = {key};
            vs.offer(new ExItem.ExIntegerItem(elems), value);
            timeSpentFeed += (System.currentTimeMillis() - startTime);
        }

        public void merge(TestDataConsumer another) {
            long startTime = System.currentTimeMillis();
            vs.merge(another.vs);
            timeSpentQuery += (System.currentTimeMillis() - startTime);
            timeSpentQuery += another.timeSpentQuery;

            timeSpentFeed += another.timeSpentFeed;
        }

        public List<Pair<ExItem<Integer>, Double>> getTopN(int k) {
            long startTime = System.currentTimeMillis();
            List<Counter<ExItem<Integer>>> tops = vs.topK(k);
            List<Pair<ExItem<Integer>, Double>> allRecords = Lists.newArrayList();

            for (Counter<ExItem<Integer>> counter : tops)
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

    private static class SpaceSavingConsumer extends TestDataConsumer {

        protected TopNCounterSummary<ExItem<Integer>> vs;

        public SpaceSavingConsumer(TopNCounterEnum type, int capacity) {
            this((TopNCounterSummary<ExItem<Integer>>) createTopNCounter(type, capacity));
        }

        public SpaceSavingConsumer(TopNCounterSummary<ExItem<Integer>> vs) {
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

    private static class HashMapConsumer extends TestDataConsumer {

        protected TopNPreciseCounter<ExItem<Integer>> vs;

        public HashMapConsumer() {
            this((TopNPreciseCounter<ExItem<Integer>>) createTopNCounter(TopNCounterEnum.Accurate, Integer.MAX_VALUE));
        }

        public HashMapConsumer(TopNPreciseCounter<ExItem<Integer>> vs) {
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
        Current, Accurate
    }

    private TestDataConsumer createTestDataConsumer(TopNCounterEnum type, int capacity) {
        switch (type) {
            case Accurate:
                return new HashMapConsumer();
            case Current:
            default:
                return new SpaceSavingConsumer(type, capacity);
        }
    }

    private static ITopNCounter<ExItem<Integer>> createTopNCounter(TopNCounterEnum type, int capacity) {
        switch (type) {
            case Accurate:
                return new TopNPreciseCounter<>(false);
            case Current:
            default:
                return new ExTopNCounter<>(capacity, false, nElems);
        }
    }
}
