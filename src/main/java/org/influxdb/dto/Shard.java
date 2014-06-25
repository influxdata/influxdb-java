package org.influxdb.dto;

import java.util.List;

import com.google.common.base.Objects;

/**
 * Represents a Shard.
 * 
 * @author stefan.majer [at] gmail.com
 * 
 */
public class Shard {
	private int id;
	private long startTime;
	private long endTime;
	private boolean longTerm;
	private List<Member> shards;

	/**
	 * A shard member.
	 * 
	 * @author stefan.majer [at] gmail.com
	 * 
	 */
	public static class Member {
		private List<Integer> serverIds;

		/**
		 * @return the serverIds
		 */
		public List<Integer> getServerIds() {
			return this.serverIds;
		}

		/**
		 * @param serverIds
		 *            the serverIds to set
		 */
		public void setServerIds(final List<Integer> serverIds) {
			this.serverIds = serverIds;
		}

	}

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
	 * @return the startTime
	 */
	public long getStartTime() {
		return this.startTime;
	}

	/**
	 * @param startTime
	 *            the startTime to set
	 */
	public void setStartTime(final long startTime) {
		this.startTime = startTime;
	}

	/**
	 * @return the endTime
	 */
	public long getEndTime() {
		return this.endTime;
	}

	/**
	 * @param endTime
	 *            the endTime to set
	 */
	public void setEndTime(final long endTime) {
		this.endTime = endTime;
	}

	/**
	 * @return the longTerm
	 */
	public boolean isLongTerm() {
		return this.longTerm;
	}

	/**
	 * @param longTerm
	 *            the longTerm to set
	 */
	public void setLongTerm(final boolean longTerm) {
		this.longTerm = longTerm;
	}

	/**
	 * @return the shards
	 */
	public List<Member> getShards() {
		return this.shards;
	}

	/**
	 * @param shards
	 *            the shards to set
	 */
	public void setShards(final List<Member> shards) {
		this.shards = shards;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String toString() {
		return Objects
				.toStringHelper(this.getClass())
				.add("id", this.id)
				.add("startTime", this.startTime)
				.add("endTime", this.endTime)
				.add("longTerm", this.longTerm)
				.add("shards", this.shards)
				.toString();
	}

}
