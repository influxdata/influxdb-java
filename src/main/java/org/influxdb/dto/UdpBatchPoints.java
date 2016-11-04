package org.influxdb.dto;

import com.google.common.base.Preconditions;
import com.google.common.collect.Range;

/**
 * {Purpose of This Type}
 *
 * {Other Notes Relating to This Type (Optional)}
 *
 * @author jiafu
 *
 */
public class UdpBatchPoints extends AbstractBatchPoints<UdpBatchPoints>{
	private int udpPort;

	UdpBatchPoints() {
		// Only visible in the Builder
	}
	
	/**
	 * Create a new UdpBatchPoints build to create a new UdpBatchPoints in a fluent manner-
	 *
	 * @param udpPort
	 *            the UDP port
	 * @return the Builder to be able to add further Builder calls.
	 */
	public static Builder udpPort(final int udpPort) {
		return new Builder(udpPort);
	}

	/**
	 * The Builder to create a new BatchPoints instance.
	 */
	public static final class Builder extends AbstractBatchPoints.Builder<Builder>{
		private final int udpPort;

		/**
		 * @param udpPort
		 */
		Builder(final int udpPort) {
			Preconditions.checkArgument(Range.closed(0, 65535).contains(udpPort), "UDP port should >=0 and <=65535.");
			this.udpPort = udpPort;
		}
 
		/**
		 * Create a new BatchPoints instance.
		 *
		 * @return the created BatchPoints.
		 */
		public UdpBatchPoints build() {
			UdpBatchPoints batchPoints = new UdpBatchPoints();
			batchPoints.setUdpPort(udpPort);
			for (Point point : this.points) {
				point.getTags().putAll(this.tags);
			}
			batchPoints.setPoints(this.points);
			return batchPoints;
		}
	}

	/**
	 * @return the UDP Port
	 */
	public int getUdpPort() {
		return this.udpPort;
	}

	/**
	 * @param database
	 *            the database to set
	 */
	void setUdpPort(final int udpPort) {
		this.udpPort = udpPort;
	}
	
	/**
	 * {@inheritDoc}
	 */  
	@Override
	public String toString() {
		StringBuilder stringBuilder = new StringBuilder();
		stringBuilder.append("UdpBatchPoints [udpPort=");
		stringBuilder.append(udpPort);
		stringBuilder.append(",");
		stringBuilder.append(super.toString());
		stringBuilder.append("]");
		return stringBuilder.toString();
	}
}
