package org.influxdb.dto;

/**
 * Representation of a InfluxDB scheduled_delete.
 * 
 * @author stefan.majer [at] gmail.com
 * 
 */
public class ScheduledDelete {
	private int id;
	private String regex;
	private String olderThan;
	private String runAt;

	/**
	 * @return the id
	 */
	public int getId() {
		return this.id;
	}

	/**
	 * @param id
	 *            the id to set
	 */
	public void setId(final int id) {
		this.id = id;
	}

	/**
	 * @return the regex
	 */
	public String getRegex() {
		return this.regex;
	}

	/**
	 * @param regex
	 *            the regex to set
	 */
	public void setRegex(final String regex) {
		this.regex = regex;
	}

	/**
	 * @return the olderThan
	 */
	public String getOlderThan() {
		return this.olderThan;
	}

	/**
	 * @param olderThan
	 *            the olderThan to set
	 */
	public void setOlderThan(final String olderThan) {
		this.olderThan = olderThan;
	}

	/**
	 * @return the runAt
	 */
	public String getRunAt() {
		return this.runAt;
	}

	/**
	 * @param runAt
	 *            the runAt to set
	 */
	public void setRunAt(final String runAt) {
		this.runAt = runAt;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("ScheduledDelete [id=");
		builder.append(this.id);
		builder.append(", regex=");
		builder.append(this.regex);
		builder.append(", olderThan=");
		builder.append(this.olderThan);
		builder.append(", runAt=");
		builder.append(this.runAt);
		builder.append("]");
		return builder.toString();
	}

}