package org.influxdb.exception;

/**
 * Class that maps the exception associated to a null InfluxDB tag.
 */
public class NullTagException extends InfluxDBException {

	/**
	 * The serial version unique identifier.
	 */
	private static final long serialVersionUID = 5410351166816305312L;

	/**
	 * Default constructor for this class.
	 * 
	 * Instantiates a new NullTag exception.
	 */
	public NullTagException() {
		super();
	}

	/**
	 * Instantiates a new NullTag exception with a customized error message.
	 * 
	 * @param message the customized error message.
	 */
	public NullTagException(String message) {
		super(message);
	}
}
