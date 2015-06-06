/**
 * This file is part of the source code and related artifacts
 * for Nimbus Application.
 *
 * Copyright © 2014 Finanz Informatik Technologie Service GmbH & Co. KG
 *
 * https://www.f-i-ts.de
 *
 * Repository path:    $HeadURL$
 * Last committed:     $Revision$
 * Last changed by:    $Author$
 * Last changed date:  $Date$
 * ID:            	   $Id$
 */
package org.influxdb.dto;

/**
 * Represents a Query against Influxdb.
 *
 * @author stefan.majer [at] gmail.com
 *
 */
public class Query {

	private final String command;
	private final String database;

	/**
	 * @param command
	 * @param database
	 */
	public Query(final String command, final String database) {
		super();
		this.command = command;
		this.database = database;
	}

	/**
	 * @return the command
	 */
	public String getCommand() {
		return this.command;
	}

	/**
	 * @return the database
	 */
	public String getDatabase() {
		return this.database;
	}

}
