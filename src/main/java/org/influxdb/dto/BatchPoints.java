/**
 * This file is part of the source code and related artifacts
 * for Nimbus Application.
 *
 * Copyright © 2014 Finanz Informatik Technologie Service GmbH & Co. KG
 *
 * https://www.f-i-ts.de
 *
 * Repository path:    $HeadURL$
 * Last committed:     $Revision$
 * Last changed by:    $Author$
 * Last changed date:  $Date$
 * ID:            	   $Id$
 */
package org.influxdb.dto;

import java.util.List;
import java.util.Map;

/**
 * {Purpose of This Type}
 *
 * {Other Notes Relating to This Type (Optional)}
 *
 * @author stefan
 *
 */
public class BatchPoints {
	private String database;
	private String retentionPolicy;
	private Map<String, String> tags;
	private Long time;
	private String precision;
	private List<Point> points;

	// private static final String TIMESTAMP_DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSS'Z'";
	// final SimpleDateFormat isoFormatter = new SimpleDateFormat(TIMESTAMP_DATE_FORMAT, LOCALE);
	// isoFormatter.setTimeZone(TimeZone.getTimeZone(UTC));
	// isoFormatter.format(new Date())

	/**
	 * @return the database
	 */
	public String getDatabase() {
		return this.database;
	}

	/**
	 * @param database
	 *            the database to set
	 */
	public void setDatabase(final String database) {
		this.database = database;
	}

	/**
	 * @return the retentionPolicy
	 */
	public String getRetentionPolicy() {
		return this.retentionPolicy;
	}

	/**
	 * @param retentionPolicy
	 *            the retentionPolicy to set
	 */
	public void setRetentionPolicy(final String retentionPolicy) {
		this.retentionPolicy = retentionPolicy;
	}

	/**
	 * @return the points
	 */
	public List<Point> getPoints() {
		return this.points;
	}

	/**
	 * @param points
	 *            the points to set
	 */
	public void setPoints(final List<Point> points) {
		this.points = points;
	}

	/**
	 * @return the tags
	 */
	public Map<String, String> getTags() {
		return this.tags;
	}

	/**
	 * @param tags
	 *            the tags to set
	 */
	public void setTags(final Map<String, String> tags) {
		this.tags = tags;
	}

	/**
	 * @return the time
	 */
	public Long getTime() {
		return this.time;
	}

	/**
	 * @param time
	 *            the time to set
	 */
	public void setTime(final Long time) {
		this.time = time;
	}

	/**
	 * @return the precision
	 */
	public String getPrecision() {
		return this.precision;
	}

	/**
	 * @param precision
	 *            the precision to set
	 */
	public void setPrecision(final String precision) {
		this.precision = precision;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("BatchPoints [database=");
		builder.append(this.database);
		builder.append(", retentionPolicy=");
		builder.append(this.retentionPolicy);
		builder.append(", tags=");
		builder.append(this.tags);
		builder.append(", time=");
		builder.append(this.time);
		builder.append(", precision=");
		builder.append(this.precision);
		builder.append(", points=");
		builder.append(this.points);
		builder.append("]");
		return builder.toString();
	}

}
