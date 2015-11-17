package org.influxdb;

import org.influxdb.impl.InfluxDBImpl;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import java.util.concurrent.TimeUnit;

/**
 * A Factory to create a instance of a InfluxDB Database adapter.
 * 
 * @author stefan.majer [at] gmail.com
 * 
 */
public enum InfluxDBFactory {
	;
	/**
	 * Create a connection to a InfluxDB.
	 * 
	 * @param url
	 *            the url to connect to.
	 * @param username
	 *            the username which is used to authorize against the influxDB instance.
	 * @param password
	 *            the password for the username which is used to authorize against the influxDB
	 *            instance.
	 * @return a InfluxDB adapter suitable to access a InfluxDB.
	 */
	public static InfluxDB connect(final String url, final String username, final String password) {
		return connect(url, username, password, 0, null);
	}

	/**
	 * Create a connection to a InfluxDB.
	 *
	 * @param url
	 *            the url to connect to.
	 * @param username
	 *            the username which is used to authorize against the influxDB instance.
	 * @param password
	 *            the password for the username which is used to authorize against the influxDB
	 *            instance.
	 * @param networkTimeout
	 *            the period of time to try to connect to the influxDB database
	 * @param timeoutTimeUnit
	 *            the unit of time that the network timeout parameter is measured
	 * @return a InfluxDB adapter suitable to access a InfluxDB.
	 */
	public static InfluxDB connect(final String url, final String username, final String password, final long networkTimeout, final TimeUnit timeoutTimeUnit) {
		Preconditions.checkArgument(!Strings.isNullOrEmpty(url), "The URL may not be null or empty.");
		Preconditions.checkArgument(!Strings.isNullOrEmpty(username), "The username may not be null or empty.");
		return new InfluxDBImpl(url, username, password, networkTimeout, timeoutTimeUnit);
	}

}
