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

import java.io.Serializable;
import java.util.Arrays;

public abstract class ExItem<T> implements Serializable {
    public final T[] elems;
    private final Integer[] intElems;

    protected ExItem(T[] elems) {
        this.elems = elems;
        if (elems[0] instanceof Integer) {
            this.intElems = (Integer[]) elems;
        } else {
            this.intElems = new Integer[elems.length];
            for (int i = 0; i < elems.length; i++) {
                this.intElems[i] = calIntElem(i);
            }
        }
    }

    public int getIntElem(int i) {
        return intElems[i];
    }

    protected abstract int calIntElem(int i);

    @Override
    public String toString() {
        return "ExItem{" +
                "elems=" + Arrays.toString(elems) +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ExItem<?> exItem = (ExItem<?>) o;
        return Arrays.equals(elems, exItem.elems);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(elems);
    }

    public static class ExIntegerItem extends ExItem<Integer> {

        public ExIntegerItem(Integer[] elems) {
            super(elems);
        }

        protected int calIntElem(int i) {
            return elems[i];
        }
    }

    public static class ExByteArrayItem extends ExItem<ByteArray> {

        public ExByteArrayItem(ByteArray[] elems) {
            super(elems);
        }

        protected int calIntElem(int i) {
            return BytesUtil.readUnsigned(elems[i].array(), elems[i].offset(), elems[i].length());
        }
    }
}
