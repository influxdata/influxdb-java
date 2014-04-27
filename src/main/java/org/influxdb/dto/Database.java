package org.influxdb.dto;

public  class Database {
	private final String name;
	private final int replicationFactor;

	public Database(final String name, final int replicationFactor) {
		super();
		this.name = name;
		this.replicationFactor = replicationFactor;
	}

	public String getName() {
		return this.name;
	}

	public int getReplicationFactor() {
		return this.replicationFactor;
	}

}