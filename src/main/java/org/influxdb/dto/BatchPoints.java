package org.influxdb.dto;

import org.influxdb.InfluxDB.ConsistencyLevel;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

/**
 * {Purpose of This Type}
 *
 * {Other Notes Relating to This Type (Optional)}
 *
 * @author stefan
 *
 */
public class BatchPoints extends AbstractBatchPoints<BatchPoints>{
	private String database;
	private String retentionPolicy;
	private ConsistencyLevel consistency;

	BatchPoints() {
		// Only visible in the Builder
	}

	/**
	 * Create a new BatchPoints build to create a new BatchPoints in a fluent manner-
	 *
	 * @param database
	 *            the name of the Database
	 * @return the Builder to be able to add further Builder calls.
	 */
	public static Builder database(final String database) {
		return new Builder(database);
	}

	/**
	 * The Builder to create a new BatchPoints instance.
	 */
	public static final class Builder extends AbstractBatchPoints.Builder<Builder>{
		private final String database;
		private String retentionPolicy;
		private ConsistencyLevel consistency;

		/**
		 * @param database
		 */
		Builder(final String database) {
			this.database = database;
		}

		/**
		 * The retentionPolicy to use.
		 *
		 * @param policy
		 * @return the Builder instance
		 */
		public Builder retentionPolicy(final String policy) {
			this.retentionPolicy = policy;
			return this;
		}
  
		/**
		 * Set the ConsistencyLevel to use. If not given it defaults to {@link ConsistencyLevel#ONE}
		 *
		 * @param consistencyLevel
		 * @return the Builder instance
		 */
		public Builder consistency(final ConsistencyLevel consistencyLevel) {
			this.consistency = consistencyLevel;
			return this;
		}

		/**
		 * Create a new BatchPoints instance.
		 *
		 * @return the created BatchPoints.
		 */
		public BatchPoints build() {
			Preconditions.checkArgument(!Strings.isNullOrEmpty(this.database), "Database must not be null or empty.");
			BatchPoints batchPoints = new BatchPoints();
			batchPoints.setDatabase(this.database);
			for (Point point : this.points) {
				point.getTags().putAll(this.tags);
			}
			batchPoints.setPoints(this.points);
			batchPoints.setRetentionPolicy(this.retentionPolicy);
			batchPoints.setTags(this.tags);
			if (null == this.consistency) {
				this.consistency = ConsistencyLevel.ONE;
			}
			batchPoints.setConsistency(this.consistency);
			return batchPoints;
		}
	}

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
	void setDatabase(final String database) {
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
	void setRetentionPolicy(final String retentionPolicy) {
		this.retentionPolicy = retentionPolicy;
	}

	/**
	 * @return the consistency
	 */
	public ConsistencyLevel getConsistency() {
		return this.consistency;
	}

	/**
	 * @param consistency
	 *            the consistency to set
	 */
	void setConsistency(final ConsistencyLevel consistency) {
		this.consistency = consistency;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String toString() {
		StringBuilder stringBuilder = new StringBuilder();
		stringBuilder.append("BatchPoints [database=");
		stringBuilder.append(database);
		stringBuilder.append(", retentionPolicy=");
		stringBuilder.append(retentionPolicy);
		stringBuilder.append(", consistency=");
		stringBuilder.append(consistency);
		stringBuilder.append(",");
		stringBuilder.append(super.toString());
		stringBuilder.append("]");
		return stringBuilder.toString();
	}

}
