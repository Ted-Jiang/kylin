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

package org.apache.kylin.storage;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.threadlocal.InternalThreadLocal;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.common.util.ImplementationSwitch;
import org.apache.kylin.metadata.model.IStorageAware;
import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.shaded.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 */
public class StorageFactory {

    private static final Logger logger = LoggerFactory.getLogger(StorageFactory.class);

    // Use thread-local because KylinConfig can be thread-local and implementation might be different among multiple threads.
    private static InternalThreadLocal<ImplementationSwitch<IStorage>> storages = new InternalThreadLocal<>();

    private static IStorage configuredUseLocalStorage;

    private static ExecutorService loadingLookupTablePool = null;

    static {
        Runtime.getRuntime().addShutdownHook(new Thread(StorageFactory::closeLoadingLookupTablePool));

        String localStorageImpl = KylinConfig.getInstanceFromEnv().getLocalStorageImpl();
        if (localStorageImpl != null) {
            configuredUseLocalStorage = (IStorage) ClassUtil.newInstance(localStorageImpl);
        }
    }

    public static ExecutorService getLoadingLookupTablePool() {
        if (loadingLookupTablePool != null) {
            return loadingLookupTablePool;
        }

        synchronized (StorageFactory.class) {
            if (loadingLookupTablePool != null) {
                return loadingLookupTablePool;
            }

            KylinConfig config = KylinConfig.getInstanceFromEnv();

            int maxThreads = config.getLoadingLookupTableMaxThreads();
            int coreThreads = config.getLoadingLookupTableCoreThreads();
            long keepAliveTime = config.getLoadingLookupTableThreadPoolAliveSeconds();
            LinkedBlockingQueue<Runnable> workQueue = new LinkedBlockingQueue<>(maxThreads * 100);
            ThreadPoolExecutor tpe = new ThreadPoolExecutor(coreThreads, maxThreads, keepAliveTime, TimeUnit.SECONDS,
                    workQueue, new ThreadFactoryBuilder().setNameFormat(("kylin-loading-lookup-%d")).build());
            tpe.allowCoreThreadTimeOut(true);

            logger.info("Creating coprocessor thread pool with max of {}, core of {}", maxThreads, coreThreads);

            loadingLookupTablePool = tpe;
            return loadingLookupTablePool;
        }
    }

    private static void closeLoadingLookupTablePool() {
        if (loadingLookupTablePool == null)
            return;

        loadingLookupTablePool.shutdown();
        try {
            if (!loadingLookupTablePool.awaitTermination(10, TimeUnit.SECONDS)) {
                loadingLookupTablePool.shutdownNow();
            }
        } catch (InterruptedException e) {
            loadingLookupTablePool.shutdownNow();
        }
    }

    public static IStorage storage(IStorageAware aware) {
        if (configuredUseLocalStorage != null) {
            return configuredUseLocalStorage;
        }
        ImplementationSwitch<IStorage> current = storages.get();
        if (storages.get() == null) {
            current = new ImplementationSwitch<>(KylinConfig.getInstanceFromEnv().getStorageEngines(), IStorage.class);
            storages.set(current);
        }
        return current.get(aware.getStorageType());
    }

    public static void clearCache() {
        storages.remove();
    }

    public static IStorageQuery createQuery(IRealization realization) {
        return storage(realization).createQuery(realization);
    }

    public static <T> T createEngineAdapter(IStorageAware aware, Class<T> engineInterface) {
        return storage(aware).adaptToBuildEngine(engineInterface);
    }

}
