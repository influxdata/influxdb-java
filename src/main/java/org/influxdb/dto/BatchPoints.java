package org.influxdb.dto;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import org.influxdb.InfluxDB.ConsistencyLevel;

import java.util.Arrays;
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
	private List<Point> points;
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
	public static Builder database(String database) {
		return new Builder(database);
	}

	/**
	 * The Builder to create a new BatchPoints instance.
	 */
	public static final class Builder {
		private final String database;
		private String retentionPolicy;
		private final Map<String, String> tags = Maps.newTreeMap(Ordering.natural());
		private final List<Point> points = Lists.newArrayList();
		private ConsistencyLevel consistency;

		/**
		 * @param database
		 */
		Builder(String database) {
			this.database = database;
		}

		/**
		 * The retentionPolicy to use.
		 *
		 * @param policy
		 * @return the Builder instance
		 */
		public Builder retentionPolicy(String policy) {
         retentionPolicy = policy;
			return this;
		}

		/**
		 * Add a tag to this set of points.
		 *
		 * @param tagName
		 *            the tag name
		 * @param value
		 *            the tag value
		 * @return the Builder instance.
		 */
		public Builder tag(String tagName, String value) {
         tags.put(tagName, value);
			return this;
		}

		/**
		 * Add a Point to this set of points.
		 *
		 * @param pointToAdd
		 * @return the Builder instance
		 */
		public Builder point(Point pointToAdd) {
         points.add(pointToAdd);
			return this;
		}

		/**
		 * Add a set of Points to this set of points.
		 *
		 * @param pointsToAdd
		 * @return the Builder instance
		 */
		public Builder points(Point... pointsToAdd) {
         points.addAll(Arrays.asList(pointsToAdd));
			return this;
		}

		/**
		 * Set the ConsistencyLevel to use. If not given it defaults to {@link ConsistencyLevel#ONE}
		 *
		 * @param consistencyLevel
		 * @return the Builder instance
		 */
		public Builder consistency(ConsistencyLevel consistencyLevel) {
         consistency = consistencyLevel;
			return this;
		}

		/**
		 * Create a new BatchPoints instance.
		 *
		 * @return the created BatchPoints.
		 */
		public BatchPoints build() {
			Preconditions.checkArgument(!Strings.isNullOrEmpty(database), "Database must not be null or empty.");
			BatchPoints batchPoints = new BatchPoints();
			batchPoints.setDatabase(database);
			for (Point point : points) {
				point.getTags().putAll(tags);
			}
			batchPoints.setPoints(points);
			batchPoints.setRetentionPolicy(retentionPolicy);
			batchPoints.setTags(tags);
			if (null == consistency) {
            consistency = ConsistencyLevel.ONE;
			}
			batchPoints.setConsistency(consistency);
			return batchPoints;
		}
	}

	/**
	 * @return the database
	 */
	public String getDatabase() {
		return database;
	}

	/**
	 * @param database
	 *            the database to set
	 */
	void setDatabase(String database) {
		this.database = database;
	}

	/**
	 * @return the retentionPolicy
	 */
	public String getRetentionPolicy() {
		return retentionPolicy;
	}

	/**
	 * @param retentionPolicy
	 *            the retentionPolicy to set
	 */
	void setRetentionPolicy(String retentionPolicy) {
		this.retentionPolicy = retentionPolicy;
	}

	/**
	 * @return the points
	 */
	public List<Point> getPoints() {
		return points;
	}

	/**
	 * @param points
	 *            the points to set
	 */
	void setPoints(List<Point> points) {
		this.points = points;
	}

	/**
	 * Add a single Point to these batches.
	 *
	 * @param point
	 * @return this Instance to be able to daisy chain calls.
	 */
	public BatchPoints point(Point point) {
		point.getTags().putAll(tags);
      points.add(point);
		return this;
	}

	/**
	 * @return the tags
	 */
	public Map<String, String> getTags() {
		return tags;
	}

	/**
	 * @param tags
	 *            the tags to set
	 */
	void setTags(Map<String, String> tags) {
		this.tags = tags;
	}

	/**
	 * @return the consistency
	 */
	public ConsistencyLevel getConsistency() {
		return consistency;
	}

	/**
	 * @param consistency
	 *            the consistency to set
	 */
	void setConsistency(ConsistencyLevel consistency) {
		this.consistency = consistency;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("BatchPoints [database=");
		builder.append(database);
		builder.append(", retentionPolicy=");
		builder.append(retentionPolicy);
		builder.append(", tags=");
		builder.append(tags);
		builder.append(", points=");
		builder.append(points);
		builder.append("]");
		return builder.toString();
	}

	// measurement[,tag=value,tag2=value2...] field=value[,field2=value2...] [unixnano]
	/**
	 * calculate the lineprotocol for all Points.
	 *
	 * @return the String with newLines.
	 */
	public String lineProtocol() {
		StringBuilder sb = new StringBuilder();
		for (Point point : points) {
			sb.append(point.lineProtocol()).append("\n");
		}
		return sb.toString();
	}
}
