package org.influxdb.dto;

import com.google.common.base.Objects;

/**
 * Represents a ShardSpace.
 * 
 * @author stefan.majer [at] gmail.com
 * 
 */
public class ShardSpace {

	private final String name;
	private String database;
	private final String retentionPolicy;
	private final String shardDuration;
	private final String regex;
	private final int replicationFactor;
	private final int split;

	/**
	 * @param name
	 * @param retentionPolicy
	 * @param shardDuration
	 * @param regex
	 * @param replicationFactor
	 * @param split
	 */
	public ShardSpace(final String name, final String retentionPolicy, final String shardDuration, final String regex,
			final int replicationFactor, final int split) {
		super();
		this.name = name;
		this.retentionPolicy = retentionPolicy;
		this.shardDuration = shardDuration;
		this.regex = regex;
		this.replicationFactor = replicationFactor;
		this.split = split;
	}

	/**
	 * @return the name
	 */
	public String getName() {
		return this.name;
	}

	/**
	 * @return the database
	 */
	public String getDatabase() {
		return this.database;
	}

	/**
	 * @return the retentionPolicy
	 */
	public String getRetentionPolicy() {
		return this.retentionPolicy;
	}

	/**
	 * @return the shardDuration
	 */
	public String getShardDuration() {
		return this.shardDuration;
	}

	/**
	 * @return the regex
	 */
	public String getRegex() {
		return this.regex;
	}

	/**
	 * @return the replicationFactor
	 */
	public int getReplicationFactor() {
		return this.replicationFactor;
	}

	/**
	 * @return the split
	 */
	public int getSplit() {
		return this.split;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String toString() {
		return Objects
				.toStringHelper(this.getClass())
				.add("name", this.name)
				.add("database", this.database)
				.add("retentionPolicy", this.retentionPolicy)
				.add("shardDuration", this.shardDuration)
				.add("regex", this.regex)
				.add("replicationFactor", this.replicationFactor)
				.add("split", this.split)
				.toString();
	}

}
