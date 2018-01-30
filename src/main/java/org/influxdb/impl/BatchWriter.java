package org.influxdb.impl;

import org.influxdb.dto.BatchPoints;

import java.util.Collection;

/**
 * Write individual batches to InfluxDB.
 */
interface BatchWriter {
  /**
   * Write the given batch into InfluxDB.
   * @param batchPointsCollection to write
   */
  void write(Collection<BatchPoints> batchPointsCollection);

  /**
   * FLush all cached writes into InfluxDB. The application is about to exit.
   */
  void close();
}

