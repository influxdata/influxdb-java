package org.influxdb.impl;

import org.influxdb.dto.BatchPoints;

/**
 * Write individual batches to InfluxDB.
 */
interface BatchWriter {
  /**
   * Write the given batch into InfluxDB.
   * @param batchPoints to write
   */
  void write(BatchPoints batchPoints);
}

