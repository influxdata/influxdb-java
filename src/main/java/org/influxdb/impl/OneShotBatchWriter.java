package org.influxdb.impl;

import org.influxdb.dto.BatchPoints;

import java.util.Collection;

/**
 * Batch writer that tries to write BatchPoints exactly once.
 */
class OneShotBatchWriter implements BatchWriter {

  private InfluxDBImpl influxDB;

  OneShotBatchWriter(final InfluxDBImpl influxDB) {
    this.influxDB = influxDB;
  }

  @Override
  public void write(final Collection<BatchPoints> batchPointsCollection) {
    for (BatchPoints batchPoints : batchPointsCollection) {
      influxDB.writeNoRetry(batchPoints);
    }
  }

  @Override
  public void close() {

  }
}
