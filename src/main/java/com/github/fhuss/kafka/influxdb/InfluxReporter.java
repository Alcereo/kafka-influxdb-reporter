/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.fhuss.kafka.influxdb;

import com.yammer.metrics.core.*;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.reporting.AbstractPollingReporter;
import com.yammer.metrics.stats.Snapshot;
import org.influxdb.dto.Point;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;

import static com.github.fhuss.kafka.influxdb.MetricsPredicate.Measures.*;

class InfluxReporter extends AbstractPollingReporter implements MetricProcessor<InfluxReporter.Context> {

    private static final Logger LOG = LoggerFactory.getLogger(InfluxReporter.class);

    private static final MetricPredicate DEFAULT_METRIC_PREDICATE = MetricPredicate.ALL;

    private List<Point> nextBatchPoints;

    private InfluxDBClient client;

    private Clock clock;

    private Context context;

    private MetricsPredicate predicate;

    private VirtualMachineMetrics vm = VirtualMachineMetrics.getInstance();

    /**
     * Creates a new {@link AbstractPollingReporter} instance.
     **/
    InfluxReporter(MetricsRegistry registry, String name, InfluxDBClient client, MetricsPredicate predicate) {
        super(registry, name);
        this.client = client;
        this.clock = Clock.defaultClock();
        this.predicate = predicate;
        this.context = new Context() {
            @Override
            public long getTime() {
                return InfluxReporter.this.clock.time();
            }
        };
    }

    @Override
    public void run() {
        try {
            this.nextBatchPoints = new LinkedList<>();
            printRegularMetrics(context);
            processVirtualMachine(vm, context);
            this.client.write(nextBatchPoints);
        } catch (Exception e) {
            LOG.error("Cannot send metrics to InfluxDB {}", e);
        }
    }

    private void printRegularMetrics(final Context context) {

        for (Map.Entry<String, SortedMap<MetricName, Metric>> entry : getMetricsRegistry().groupedMetrics(DEFAULT_METRIC_PREDICATE).entrySet()) {
            for (Map.Entry<MetricName, Metric> subEntry : entry.getValue().entrySet()) {
                final Metric metric = subEntry.getValue();
                if (metric != null) {
                    try {
                        metric.processWith(this, subEntry.getKey(), context);
                    } catch (Exception ignored) {
                        LOG.error("Error printing regular metrics:", ignored);
                    }
                }
            }
        }
    }

    private void processPoint(MetricName name, Context context, PointedConsumer consumer){
        Point.Builder point = buildPoint(name, context);
        consumer.consumePointBuilder(point);
        nextBatchPoints.add(point.build());
    }

    @Override
    public void processMeter(MetricName name, Metered meter, Context context) throws Exception {
        processPoint(name, context, pointBuilder ->
                addMeteredFields(meter, pointBuilder)
        );
    }

    @Override
    public void processCounter(MetricName name, Counter counter, Context context) throws Exception {
        processPoint(name, context, pointBuilder ->
                filterOrAddField(count, pointBuilder, counter.count())
        );
    }

    @Override
    public void processHistogram(MetricName name, Histogram histogram, Context context) throws Exception {
        final Snapshot snapshot = histogram.getSnapshot();
        processPoint(name, context, pointBuilder -> {
            addSummarizableFields(histogram, pointBuilder);
            addSnapshotFields(snapshot, pointBuilder);
        });
    }

    @Override
    public void processTimer(MetricName name, Timer timer, Context context) throws Exception {
        final Snapshot snapshot = timer.getSnapshot();

        processPoint(name, context, pointBuilder -> {
            addSummarizableFields(timer, pointBuilder);
            addMeteredFields(timer, pointBuilder);
            addSnapshotFields(snapshot, pointBuilder);
            filterOrAddField(count, pointBuilder, timer.count());
        });
    }

    @Override
    public void processGauge(MetricName name, Gauge<?> gauge, Context context) throws Exception {

        processPoint(name, context, pointBuilder -> {
            Object fieldValue = gauge.value();
            String fieldName = value.label();
            if (fieldValue instanceof Float)
                pointBuilder.addField(fieldName, (Float) fieldValue);
            else if (fieldValue instanceof Double)
                pointBuilder.addField(fieldName, (Double) fieldValue);
            else if (fieldValue instanceof Long)
                pointBuilder.addField(fieldName, (Long) fieldValue);
            else if (fieldValue instanceof Integer)
                pointBuilder.addField(fieldName, (Integer) fieldValue);
            else if (fieldValue instanceof String)
                pointBuilder.addField(fieldName, (String) fieldValue);
            else
                return;
        });
    }


    private void addSummarizableFields(Summarizable m, Point.Builder point) {
        filterOrAddField(max, point, m.max());
        filterOrAddField(mean, point, m.mean());
        filterOrAddField(min, point, m.min());
        filterOrAddField(stddev, point, m.stdDev());
        filterOrAddField(sum, point, m.sum());
    }

    private void addMeteredFields(Metered m, Point.Builder point) {
        filterOrAddField(m1Rate, point, m.oneMinuteRate());
        filterOrAddField(m5Rate, point, m.fiveMinuteRate());
        filterOrAddField(m15Rate, point, m.fifteenMinuteRate());
        filterOrAddField(meanRate, point, m.meanRate());
        filterOrAddField(count, point, m.count());

    }

    private void addSnapshotFields(Snapshot m, Point.Builder point) {
        filterOrAddField(median, point, m.getMedian());
        filterOrAddField(p75, point, m.get75thPercentile());
        filterOrAddField(p95, point, m.get95thPercentile());
        filterOrAddField(p98, point, m.get98thPercentile());
        filterOrAddField(p99, point, m.get99thPercentile());
        filterOrAddField(p999, point, m.get999thPercentile());
    }

    public void processVirtualMachine(VirtualMachineMetrics vm, Context context){

        MetricName metricName = new MetricName("JVM", "JVM", "JVM");

        processPoint(metricName, context, pointBuilder -> {
            pointBuilder.addField("memory.heap_used", vm.heapUsed());
            pointBuilder.addField("memory.heap_usage", vm.heapUsage());
            pointBuilder.addField("memory.non_heap_usage", vm.nonHeapUsage());

            pointBuilder.addField("daemon_thread_count", vm.daemonThreadCount());
            pointBuilder.addField("thread_count", vm.threadCount());
            pointBuilder.addField("uptime", vm.uptime());
            pointBuilder.addField("fd_usage", vm.fileDescriptorUsage());
        });
    }

    private void filterOrAddField(MetricsPredicate.Measures measure, Point.Builder point, double value) {
        if (predicate.isEnable(measure)) point.addField(measure.label(), value);
    }

    private Point.Builder buildPoint(MetricName name, Context context) {
        Point.Builder pb = Point.measurement(name.getType())
                .time(context.getTime(), TimeUnit.MILLISECONDS)
                .tag("metric", name.getName());
//                .addField("group", name.getGroup());

        if( name.hasScope() ) {
            String scope = name.getScope();

            List<String> scopes = Arrays.asList(scope.split("\\."));
            if( scopes.size() % 2 == 0) {
                Iterator<String> iterator = scopes.iterator();
                while (iterator.hasNext()) {
                    pb.tag(iterator.next(), iterator.next());
                }
            }
            else pb.tag("scope", scope);
        }
        return pb;
    }

    public void close(){
        client.close();
    }

    @FunctionalInterface
    public interface PointedConsumer{
        void consumePointBuilder(Point.Builder pointBuilder);
    }

    public interface Context {

        long getTime( );
    }
}
