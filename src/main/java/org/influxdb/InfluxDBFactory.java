package org.influxdb;

import org.influxdb.impl.InfluxDBImpl;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

/**
 * A Interface to a InfluxDB Database.
 * 
 * @author stefan.majer [at] gmail.com
 * 
 */
public enum InfluxDBFactory {
	;
	public static InfluxDB connect(final String url, final String username, final String password) {
		Preconditions.checkArgument(!Strings.isNullOrEmpty(url), "The URL may not be null or empty.");
		Preconditions.checkArgument(!Strings.isNullOrEmpty(username), "The username may not be null or empty.");
		return new InfluxDBImpl(url, username, password);
	}

}
