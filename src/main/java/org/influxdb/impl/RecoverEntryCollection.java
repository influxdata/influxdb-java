package org.influxdb.impl;

import org.influxdb.dto.Point;

import java.util.Comparator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;

/**
 * Created by Paul on 11/17/2015.
 */
public class RecoverEntryCollection {
    private final int recoverLimit;
    private final BlockingQueue<BatchEntry> queue;

    public RecoverEntryCollection(final int recoverLimit) {
        this.recoverLimit = recoverLimit;
        queue = new PriorityBlockingQueue<>(recoverLimit != Integer.MAX_VALUE ? recoverLimit : 1000000, new Comparator<BatchEntry>() {
            @Override
            public int compare(BatchEntry o1, BatchEntry o2) {
                return o1.getPoint().getTime().compareTo(o2.getPoint().getTime());
            }
        });
    }

    public void add(final String databaseName, final String retentionPolicy, final List<Point> points) {
        // The only problem here is that the queue will fall apart if the recoverLimit is Integer.MAX_VALUE
        // and the queue has already reached that limit. However, that will likely cause an OOM exception

        for(Point point : points)
            queue.add(new BatchEntry(point, databaseName, retentionPolicy));

        final int toRemove = queue.size() - recoverLimit;
        if (toRemove > 0) {
            for(int ii=0;ii<toRemove;++ii)
                queue.remove(); // Remove the oldest items from the queue
        }

    }
    public List<BatchEntry> drainEntries() {
        if (queue.isEmpty())
            return null;

        List<BatchEntry> drainToList = null;
        queue.drainTo(drainToList);
        return drainToList;
    }
}
