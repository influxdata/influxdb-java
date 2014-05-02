package org.influxdb.dto;

import com.google.common.base.Objects;

/**
 * Representation of a InfluxDB database user.
 * 
 * @author stefan.majer [at] gmail.com
 * 
 */
public class User {
	private final String name;
	private String password;
	private boolean admin;
	private String readFrom;
	private String writeTo;

	/**
	 * @param name
	 *            the name of the user.
	 */
	public User(final String name) {
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
	 * @return the password
	 */
	public String getPassword() {
		return this.password;
	}

	/**
	 * @param password
	 *            the password to set
	 */
	public void setPassword(final String password) {
		this.password = password;
	}

	/**
	 * @return the admin
	 */
	public boolean isAdmin() {
		return this.admin;
	}

	/**
	 * @param admin
	 *            the admin to set
	 */
	public void setAdmin(final boolean admin) {
		this.admin = admin;
	}

	/**
	 * @return the readFrom
	 */
	public String getReadFrom() {
		return this.readFrom;
	}

	/**
	 * @param readFrom
	 *            the readFrom to set
	 */
	public void setReadFrom(final String readFrom) {
		this.readFrom = readFrom;
	}

	/**
	 * @return the writeTo
	 */
	public String getWriteTo() {
		return this.writeTo;
	}

	/**
	 * @param writeTo
	 *            the writeTo to set
	 */
	public void setWriteTo(final String writeTo) {
		this.writeTo = writeTo;
	}

	/**
	 * Setter for readFrom and writeTo permissions for this user.
	 * 
	 * @param permissions
	 *            a array of permissions, can be either skipped or exactly 2 readFrom and writeTo in
	 *            this order.
	 */
	public void setPermissions(final String... permissions) {
		if (null != permissions) {
			switch (permissions.length) {
			case 0:
				break;
			case 2:
				this.setReadFrom(permissions[0]);
				this.setWriteTo(permissions[1]);
				break;
			default:
				throw new IllegalArgumentException("You have to specify readFrom and writeTo permissions.");
			}
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String toString() {
		return Objects
				.toStringHelper(this.getClass())
				.add("name", this.name)
				.add("password", this.password)
				.add("admin", this.admin)
				.add("readFrom", this.readFrom)
				.add("writeTo", this.writeTo)
				.toString();
	}

}