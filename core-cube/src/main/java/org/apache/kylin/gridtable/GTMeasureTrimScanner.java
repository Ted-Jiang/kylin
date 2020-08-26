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

package org.apache.kylin.gridtable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class GTMeasureTrimScanner implements IGTScanner {

    private final IGTScanner rawScanner;
    private final GTInfo info;
    private final List<Integer> directReturnResultColumns;

    private GTRecord next = null;

    public GTMeasureTrimScanner(IGTScanner rawScanner, List<Integer> directReturnResultColumns) {
        this.rawScanner = rawScanner;
        this.info = rawScanner.getInfo();
        this.directReturnResultColumns = directReturnResultColumns;
    }

    @Override
    public GTInfo getInfo() {
        return info;
    }

    @Override
    public void close() throws IOException {
        rawScanner.close();
    }

    @Override
    public Iterator<GTRecord> iterator() {
        return new Iterator<GTRecord>() {
            private Iterator<GTRecord> inputIterator = rawScanner.iterator();

            @Override
            public boolean hasNext() {
                if (next != null)
                    return true;

                if (inputIterator.hasNext()) {
                    next = inputIterator.next();
                    trimGTRecord(next);
                    return true;
                }
                return false;
            }

            @Override
            public GTRecord next() {
                // fetch next record
                if (next == null) {
                    hasNext();
                    if (next == null)
                        throw new NoSuchElementException();
                }

                GTRecord result = next;
                next = null;
                return result;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    private void trimGTRecord(GTRecord record) {
        for (Integer i : directReturnResultColumns) {
            ByteBuffer recordBuffer = record.get(i).asBuffer();
            if (recordBuffer != null) {
                ByteBuffer trimmedBuffer = info.getCodeSystem().getSerializer(i).getFinalResult(recordBuffer);
                record.loadColumns(i, trimmedBuffer);
            }
        }
    }
}
