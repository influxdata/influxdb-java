package org.influxdb.dto;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;

/**
 * {Purpose of This Type}
 *
 * {Other Notes Relating to This Type (Optional)}
 *
 * @author stefan
 *
 */
public abstract class AbstractBatchPoints<T extends AbstractBatchPoints<T>> {
	protected Map<String, String> tags;
	protected List<Point> points;

	AbstractBatchPoints() {
		// Only visible in the Builder
	}
 
	/**
	 * The Builder to create a new BatchPoints instance.
	 */
	public abstract static class Builder<T extends Builder<T>>{
		protected final Map<String, String> tags = Maps.newTreeMap(Ordering.natural());
		protected final List<Point> points = Lists.newArrayList();


		/**
		 * Add a tag to this set of points.
		 *
		 * @param tagName
		 *            the tag name
		 * @param value
		 *            the tag value
		 * @return the Builder instance.
		 */
		public T tag(final String tagName, final String value) {
			this.tags.put(tagName, value);
			return (T) this;
		}

		/**
		 * Add a Point to this set of points.
		 *
		 * @param pointToAdd
		 * @return the Builder instance
		 */
		public T point(final Point pointToAdd) {
			this.points.add(pointToAdd);
			return (T) this;
		}

		/**
		 * Add a set of Points to this set of points.
		 *
		 * @param pointsToAdd
		 * @return the Builder instance
		 */
		public T points(final Point... pointsToAdd) {
			this.points.addAll(Arrays.asList(pointsToAdd));
			return (T) this;
		}

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
	void setPoints(final List<Point> points) {
		this.points = points;
	}

	/**
	 * Add a single Point to these batches.
	 *
	 * @param point
	 * @return this Instance to be able to daisy chain calls.
	 */
	public T point(final Point point) {
		point.getTags().putAll(this.tags);
		this.points.add(point);
		return (T) this;
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
	void setTags(final Map<String, String> tags) {
		this.tags = tags;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("tags=");
		builder.append(this.tags);
		builder.append(", points=");
		builder.append(this.points);
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
		for (Point point : this.points) {
			sb.append(point.lineProtocol()).append("\n");
		}
		return sb.toString();
	}
}
