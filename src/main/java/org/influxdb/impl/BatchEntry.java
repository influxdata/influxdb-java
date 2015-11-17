package org.influxdb.impl;

import org.influxdb.dto.Point;

/**
 * Created by Paul on 11/17/2015.
 */
public class BatchEntry {
    private final Point point;
    private final String db;
    private final String rp;

    public BatchEntry(final Point point, final String db, final String rp) {
        super();
        this.point = point;
        this.db = db;
        this.rp = rp;
    }

    public Point getPoint() {
        return this.point;
    }

    public String getDb() {
        return this.db;
    }

    public String getRp() {
        return this.rp;
    }
}
