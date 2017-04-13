package org.influxdb.exception;

public class DeleteInfluxException extends Exception {
    public DeleteInfluxException() {
    }

    public DeleteInfluxException(final String message) {
        super(message);
    }

    public DeleteInfluxException(final String message, final Throwable cause) {
        super(message, cause);
    }

    public DeleteInfluxException(final Throwable cause) {
        super(cause);
    }

    public DeleteInfluxException(final String message, final Throwable cause,
                                 final boolean enableSuppression, final boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
