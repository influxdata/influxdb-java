package org.influxdb;

import org.influxdb.dto.QueryResult;

import java.util.function.Consumer;

public class AsyncResult<T>  {

    private final Object syncObject = new Object();

    private boolean gotResult = false;
    private T result = null;
    private Throwable throwable = null;

    T result() throws Throwable {
        while (!this.gotResult) {
            synchronized (this.syncObject) {
                this.syncObject.wait();
            }
        }

        if (this.throwable != null) {
            throw this.throwable;
        }

        return this.result;
    }

    public final Consumer<T> resultConsumer = new Consumer<T>() {
        @Override
        public void accept(T t) {
            synchronized (syncObject) {
                result = t;
                gotResult = true;
                syncObject.notifyAll();
            }
        }
    };

    public final Consumer<Throwable> errorConsumer = new Consumer<Throwable>() {
        @Override
        public void accept(Throwable t) {
            synchronized (syncObject) {
                throwable = t;
                gotResult = true;
                syncObject.notifyAll();
            }
        }
    };

}
