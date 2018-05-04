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

import com.sun.management.OperatingSystemMXBean;
import com.yammer.metrics.core.*;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.reporting.AbstractPollingReporter;
import com.yammer.metrics.stats.Snapshot;
import org.influxdb.dto.Point;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.management.ManagementFactoryHelper;

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
    OperatingSystemMXBean runtimeMXBean = (OperatingSystemMXBean) ManagementFactoryHelper.getOperatingSystemMXBean();

    /**
     * Creates a new {@link AbstractPollingReporter} instance.
     **/
    InfluxReporter(
            MetricsRegistry registry,
            String name,
            InfluxDBClient client,
            MetricsPredicate predicate,
            Map<String, String> tags
    ) {
        super(registry, name);
        this.client = client;
        this.clock = Clock.defaultClock();
        this.predicate = predicate;
        this.context = new Context() {
            @Override
            public long getTime() {
                return InfluxReporter.this.clock.time();
            }

            @Override
            public Map<String, String> getTags(){
                return tags;
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
        try {
            nextBatchPoints.add(point.build());
        }catch (IllegalArgumentException exception){
            LOG.warn("Error adding point to batch. Point: {}. Description: {}", point, exception.getLocalizedMessage());
            exception.printStackTrace();
        }
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

            if (name.getName().equals("NetworkProcessorAvgIdlePercent") && name.getType().equals("SocketServer")) {
                pointBuilder.addField("percent", (Number) fieldValue);

            }else if (name.getName().equals("ClusterId") && name.getType().equals("KafkaServer")){
                pointBuilder.addField("name", fieldValue.toString());

            }else if (name.getType().equals("ReplicaFetcherManager")){
                pointBuilder.addField(fieldName, Double.valueOf(fieldValue.toString()));

            }else {

                if (fieldValue instanceof Number)
                    pointBuilder.addField(fieldName, (Number) fieldValue);

                else
                    pointBuilder.addField(fieldName, fieldValue.toString());
            }
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

        //    ---  MEMORY ---

        addJvmMeasurement(
                context,
                "JvmMemory",
                "heap_max", "bytes",
                vm.heapMax()
        );

        addJvmMeasurement(
                context,
                "JvmMemory",
                "heap_used", "bytes",
                vm.heapUsed()
        );

        addJvmMeasurement(
                context,
                "JvmMemory",
                "heap_committed", "bytes",
                vm.heapCommitted()
        );

        addJvmMeasurement(
                context,
                "JvmMemory",
                "heap_used", "percent",
                vm.heapUsage()
        );

        addJvmMeasurement(
                context,
                "JvmMemory",
                "non_heap_used", "percent",
                vm.nonHeapUsage()
        );

        addJvmMeasurement(
                context, "JvmMemory",
                "total_used", "bytes",
                vm.totalUsed()
        );

        addJvmMeasurement(
                context, "JvmMemory",
                "total_committed", "bytes",
                vm.totalCommitted()
        );

        addJvmMeasurement(
                context, "JvmMemory",
                "total_max", "bytes",
                vm.totalMax()
        );

        //    ---  THREADS ---

        addJvmMeasurement(
                context, "JvmThreads",
                "daemon", "count",
                vm.daemonThreadCount()
        );

        addJvmMeasurement(
                context, "JvmThreads",
                "all", "count",
                vm.threadCount()
        );

        addJvmMeasurement(
                context, "JvmThreads",
                "deadlocked", "count",
                vm.deadlockedThreads().size()
        );

        //    ---  THREAD STATUSES ---

        vm.threadStatePercentages().forEach((state, aDouble) ->
                addJvmMeasurement(
                        context, "JvmThreadsStatuses",
                        state.name(), "percent",
                        aDouble
                )
        );

        //    ---  CPU ---

        addJvmMeasurement(
                context, "JvmCPU",
                "process", "load",
                runtimeMXBean.getProcessCpuLoad()
        );

        addJvmMeasurement(
                context, "JvmCPU",
                "system", "load",
                runtimeMXBean.getSystemCpuLoad()
        );

    }

    private void addJvmMeasurement(Context context, String measurement, String tag, String field, Number value) {
        processPoint(
                new MetricName("JVM", measurement, tag),
                context,
                pointBuilder -> pointBuilder.addField(field, value)
        );
    }

    private void filterOrAddField(MetricsPredicate.Measures measure, Point.Builder point, double value) {
        if (predicate.isEnable(measure)) point.addField(measure.label(), value);
    }

    private Point.Builder buildPoint(MetricName name, Context context) {
        Point.Builder pointBuilder = Point.measurement(name.getType())
                .time(context.getTime(), TimeUnit.MILLISECONDS)
                .tag("metric", name.getName());

        pointBuilder.tag(context.getTags());

        if( name.hasScope() ) {
            String scope = name.getScope();

            List<String> scopes = Arrays.asList(scope.split("\\."));
            if( scopes.size() % 2 == 0) {
                Iterator<String> iterator = scopes.iterator();
                while (iterator.hasNext()) {
                    pointBuilder.tag(iterator.next(), iterator.next());
                }
            }
            else pointBuilder.tag("scope", scope);
        }

        return pointBuilder;
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

        Map<String, String> getTags();
    }
}
