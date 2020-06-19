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
package org.apache.kylin.metrics.lib.impl.prometheus;

import java.util.List;
import java.util.Properties;

import org.apache.kylin.metrics.lib.ActiveReservoir;
import org.apache.kylin.metrics.lib.ActiveReservoirListener;
import org.apache.kylin.metrics.lib.ActiveReservoirReporter;
import org.apache.kylin.metrics.lib.Record;
import org.apache.kylin.metrics.lib.impl.ReporterBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PrometheusReservoirReporter extends ActiveReservoirReporter {
    protected static final Logger logger = LoggerFactory.getLogger(PrometheusReservoirReporter.class);
    public static final String PROMETHEUS_REPORTER_SUFFIX = "PROMETHEUS";
    private final ActiveReservoir activeReservoir;
    private final PrometheusReservoirListener listener;

    public PrometheusReservoirReporter(ActiveReservoir activeReservoir, Properties props) throws Exception {
        this.activeReservoir = activeReservoir;
        this.listener = new PrometheusReservoirListener(props);
    }

    /**
     * Returns a new {@link Builder} for {@link PrometheusReservoirReporter}.
     *
     * @param activeReservoir the registry to report
     * @return a {@link Builder} instance for a {@link PrometheusReservoirReporter}
     */
    public static Builder forRegistry(ActiveReservoir activeReservoir) {
        return new Builder(activeReservoir);
    }

    /**
     * Starts the reporter.
     */
    public void start() {
        activeReservoir.addListener(listener);
    }

    /**
     * Stops the reporter.
     */
    public void stop() {
        activeReservoir.removeListener(listener);
    }

    /**
     * Stops the reporter.
     */
    @Override
    public void close() {
        stop();
    }

    protected PrometheusReservoirListener getListener() {
        return listener;
    }

    /**
     * A builder for {@link PrometheusReservoirReporter} instances.
     */
    public static class Builder extends ReporterBuilder {

        private Builder(ActiveReservoir activeReservoir) {
            super(activeReservoir);
        }

        private void setFixedProperties() {
        }

        /**
         * Builds a {@link PrometheusReservoirReporter} with the given properties.
         *
         * @return a {@link PrometheusReservoirReporter}
         */
        public PrometheusReservoirReporter build() throws Exception {
            setFixedProperties();
            return new PrometheusReservoirReporter(registry, props);
        }
    }

    protected class PrometheusReservoirListener implements ActiveReservoirListener {

        private int nRecord = 0;
        private int nRecordSkip = 0;

        PrometheusEventProducer producer;

        private PrometheusReservoirListener(Properties props) throws Exception {
            producer = new PrometheusEventProducer(props);
        }

        public boolean onRecordUpdate(final List<Record> records) {
            try {
                producer.add(records);
                nRecord += records.size();
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
                nRecordSkip += records.size();
                return false;
            }
            return true;
        }

        public void close() {
            try {
                producer.close();
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
        }

        public int getNRecord() {
            return nRecord;
        }

        public int getNRecordSkip() {
            return nRecordSkip;
        }
    }
}
