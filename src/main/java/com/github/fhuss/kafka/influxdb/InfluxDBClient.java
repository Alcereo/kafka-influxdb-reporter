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

import com.google.common.collect.Lists;
import org.influxdb.*;
import org.influxdb.dto.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

class InfluxDBClient {

    private static final Logger LOG = LoggerFactory.getLogger(InfluxDBClient.class);

    private InfluxDBMetricsConfig config;

    private boolean isDatabaseCreated = false;

    private final InfluxDB influxDB;

    /**
     * Creates a new {@link InfluxDBClient} instance.
     *
     * @param config the configuratoon.
     */
    public InfluxDBClient(InfluxDBMetricsConfig config) throws InfluxDBIOException {
        this.config = config;

        influxDB = InfluxDBFactory.connect(
                config.getConnectString(),
                config.getUsername(),
                config.getPassword()
        );

        Pong ping = influxDB.ping();

        if (ping.isGood()){
            LOG.info("Success connect to InfluxDB. Version: "+ping.getVersion());

            influxDB.enableBatch(BatchOptions.DEFAULTS.actions(2000).flushDuration(100));
        }else {
            LOG.error("Error conenction to InfluxDB.");
        }

    }

    public boolean createDatabase(String database) {
        try {
            LOG.info("Attempt to create InfluxDB database {}", database);

            QueryResult query = influxDB
                    .query(new Query("CREATE DATABASE \"" + database + "\"", ""));

            if (query.hasError()){
                LOG.error("Error create database {}. Error: {}", database, query.getError());
            }else {
                this.isDatabaseCreated = true;
            }

        } catch (Exception e) {
            LOG.error("Exception while creating database: "+database, e);
        }
        return this.isDatabaseCreated;
    }

    public boolean deleteDatabase(String database) {
        try {
            LOG.info("Attempt to delete InfluxDB database {}", database);

            QueryResult query = influxDB
                    .query(new Query("DROP DATABASE \"" + database + "\"", ""));

            if (query.hasError()){
                LOG.error("Error drop database {}. Error: {}", database, query.getError());
                return false;
            }

            return true;
        } catch (Exception e) {
            LOG.error("Exception while error database: "+database, e);

            return false;
        }
    }

    public void close(){
        influxDB.close();
    }

    public void write(List<Point> points) {
        if (this.isDatabaseCreated || createDatabase(this.config.getDatabase())) {

            Lists.partition(points, 200).forEach(pointBatchPart -> {

                BatchPoints.Builder batchBuilder = newPointsBatchBuilder(this.config);
                pointBatchPart.forEach(batchBuilder::point);

                writePointsBatch(batchBuilder.build(), influxDB);

            });
        }
    }

    private static void writePointsBatch(BatchPoints batchPoints, InfluxDB influxDB) {
        try {
            influxDB.write(batchPoints);
        }catch (InfluxDBException.FieldTypeConflictException exception){
            LOG.warn("Field type conflict exception with points: "+printPoints(batchPoints.getPoints()));
        }catch (Exception e){
            LOG.error("Error on write batch to InfluxDB", e);
        }
    }

    private static BatchPoints.Builder newPointsBatchBuilder(InfluxDBMetricsConfig config) {
        BatchPoints.Builder batchBuilder = BatchPoints.database(config.getDatabase());

        if (config.getRetention() != null)
            batchBuilder.retentionPolicy(config.getRetention());
        if (config.getConsistency() != null)
            batchBuilder.consistency(InfluxDB.ConsistencyLevel.valueOf(config.getConsistency()));

        return batchBuilder;
    }

    private static String printPoints(List<Point> points) {
        StringBuilder builder = new StringBuilder();

        builder.append("list points: ").append("\n");
        points.forEach(point -> builder.append(point).append("\n"));

        return builder.toString();
    }

}
