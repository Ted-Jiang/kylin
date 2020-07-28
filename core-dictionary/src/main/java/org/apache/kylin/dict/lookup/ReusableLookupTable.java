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

package org.apache.kylin.dict.lookup;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class ReusableLookupTable implements ILookupTable {

    private AtomicInteger usageNumber = new AtomicInteger(0);
    private volatile boolean isClosed = false;

    protected ReusableLookupTable() {
    }

    @Override
    public void increaseUsage() {
        usageNumber.incrementAndGet();
    }

    @Override
    public void close() throws IOException {
        synchronized (this) {
            if (usageNumber.decrementAndGet() <= 0) {
                closeInner();
                isClosed = true;
            }
        }
    }

    @Override
    public boolean isClosed() {
        return isClosed;
    }

    protected abstract void closeInner() throws IOException;
}
