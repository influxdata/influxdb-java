package org.influxdb.dto;

import java.util.List;

import com.google.common.base.Objects;
import com.google.common.collect.Lists;

/**
 * Configuration for a Database.
 * 
 * @author stefan.majer [at] gmail.com
 * 
 */
public class DatabaseConfiguration {
	private final String name;
	private final List<ShardSpace> spaces = Lists.newArrayList();
	private final List<String> continuousQueries = Lists.newArrayList();

	/**
	 * @param name
	 *            the name of the Database.
	 */
	public DatabaseConfiguration(final String name) {
		super();
		this.name = name;
	}

	/**
	 * @return the name
	 */
	public String getName() {
		return this.name;
	}

	/**
	 * @return the spaces
	 */
	public List<ShardSpace> getSpaces() {
		return this.spaces;
	}

	/**
	 * @param space
	 *            the space to add
	 */
	public void addSpace(final ShardSpace space) {
		this.spaces.add(space);
	}

	/**
	 * @return the continuousQueries
	 */
	public List<String> getContinuousQueries() {
		return this.continuousQueries;
	}

	/**
	 * @param continuousQuery
	 *            the continuousQuery add
	 */
	public void addContinuousQueries(final String continuousQuery) {
		this.continuousQueries.add(continuousQuery);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String toString() {
		return Objects
				.toStringHelper(this.getClass())
				.add("name", this.name)
				.add("spaces", this.spaces)
				.add("continuousQueries", this.continuousQueries)
				.toString();
	}

}
