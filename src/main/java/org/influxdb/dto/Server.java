package org.influxdb.dto;

import com.google.common.base.Objects;

/**
 * Representation of a InfluxDB Server which is part of a cluster.
 * 
 * @author stefan.majer [at] gmail.com
 * 
 */
public class Server {
	private int id;
	private String protobufConnectString;

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
	 * @return the protobufConnectString
	 */
	public String getProtobufConnectString() {
		return this.protobufConnectString;
	}

	/**
	 * @param protobufConnectString
	 *            the protobufConnectString to set
	 */
	public void setProtobufConnectString(final String protobufConnectString) {
		this.protobufConnectString = protobufConnectString;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String toString() {
		return Objects
				.toStringHelper(this.getClass())
				.add("id", this.id)
				.add("protobufConnectString", this.protobufConnectString)
				.toString();
	}

}
