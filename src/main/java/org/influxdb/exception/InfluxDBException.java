package org.influxdb.exception;

/**
 * Base class to any InfluxDB exception.
 */
public class InfluxDBException extends RuntimeException {

	/**
	 * The serial version unique identifier.
	 */
	private static final long serialVersionUID = -5147473356345660862L;

	/**
	 * Default constructor for this class.
	 * 
	 * Instantiates a new InfluxDB exception.
	 */
	public InfluxDBException() {
		super();
	}

	/**
	 * Instantiates a new InfluxDB exception with a customized error message and
	 * with the cause of the error.
	 * 
	 * @param message
	 *            the customized error message
	 * @param cause
	 *            the error cause
	 */
	public InfluxDBException(String message, Throwable cause) {
		super(message, cause);
	}

	/**
	 * Instantiates a new InfluxDB exception with a customized error message.
	 * 
	 * @param message
	 *            the customized error message
	 */
	public InfluxDBException(String message) {
		super(message);
	}

	/**
	 * Instantiates a new InfluxDB exception with the cause of the error.
	 * 
	 * @param cause
	 *            the error cause
	 */
	public InfluxDBException(Throwable cause) {
		super(cause);
	}
}
