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

import org.apache.kylin.shaded.com.google.common.base.Preconditions;

import java.util.Comparator;
import java.util.List;

public class MathUtil {

    private MathUtil() {
        throw new IllegalStateException("Class MathUtil is an utility class !");
    }

    public static double findMedianInSortedList(List<Double> m) {
        int middle = m.size() / 2;
        if (m.size() % 2 == 1) {
            return m.get(middle);
        } else {
            return (m.get(middle - 1) + m.get(middle)) / 2.0;
        }
    }

    public static <T> T findMedianElement(Comparator<? super T> comparator, List<T> list) {
        return findMedianElement(comparator, list, null);
    }

    public static <T> T findMedianElement(Comparator<? super T> comparator, List<T> list, T hint) {
        return findKthElement(comparator, list, list.size() / 2, hint);
    }

    public static <T> T findKthElement(Comparator<? super T> comparator, List<T> list, int k) {
        return findKthElement(comparator, list, k, null);
    }

    public static <T> T findKthElement(Comparator<? super T> comparator, List<T> list, int k, T hint) {
        Preconditions.checkArgument(k >= 0 && k < list.size());
        if (hint != null) {
            list.add(hint);
        }
        return findKthElementByQuickSelect(comparator, list, 0, list.size() - 1, k);
    }

    private static <T> T findKthElementByQuickSelect(Comparator<? super T> comparator, List<T> list, int left, int right, int k) {
        int pos = partition(comparator, list, left, right);
        if (pos - left + 1 == k) {
            return list.get(pos);
        } else if (pos - left + 1 > k) {
            return findKthElementByQuickSelect(comparator, list, left, pos - 1, k);
        } else {
            return findKthElementByQuickSelect(comparator, list, pos + 1, right, k - pos + left - 1);
        }
    }

    private static <T> int partition(Comparator<? super T> comparator, List<T> list, int left, int right) {
        T pivot = list.get(right);
        int storeIndex = left;
        for (int i = left; i < right; i++) {
            if (comparator.compare(list.get(i), pivot) <= 0) {
                swap(list, storeIndex, i);
                storeIndex++;
            }
        }
        swap(list, right, storeIndex);
        return storeIndex;
    }

    private static <T> void swap(List<T> list, int n1, int n2) {
        if (n1 == n2) {
            return;
        }
        T temp = list.get(n2);
        list.set(n2, list.get(n1));
        list.set(n1, temp);
    }
}
