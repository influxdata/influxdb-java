package org.influxdb.impl;

import java.util.EnumSet;
import java.util.concurrent.TimeUnit;

/**
 * Utils for time related methods.
 *
 * @author stefan.majer [at] gmail.com
 *
 */
public enum TimeUtil {
	;
	/**
	 * Convert from a TimeUnit to a influxDB timeunit String.
	 *
	 * @param t
	 * @return the String representation.
	 */
	public static String toTimePrecision(TimeUnit t) {
		switch (t) {
		case HOURS:
			return "h";
		case MINUTES:
			return "m";
		case SECONDS:
			return "s";
		case MILLISECONDS:
			return "ms";
		case MICROSECONDS:
			return "u";
		case NANOSECONDS:
			return "n";
		default:
			EnumSet<TimeUnit> allowedTimeunits = EnumSet.of(
					TimeUnit.HOURS,
					TimeUnit.MINUTES,
					TimeUnit.SECONDS,
					TimeUnit.MILLISECONDS,
					TimeUnit.MICROSECONDS,
					TimeUnit.NANOSECONDS);
			throw new IllegalArgumentException("time precision must be one of:" + allowedTimeunits);
		}
	}

}
