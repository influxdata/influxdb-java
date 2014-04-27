package org.influxdb.dto;

public  class User {
	private String name;
	private String password;
	private boolean admin;
	private String readFrom;
	private String writeTo;

	public String getName() {
		return this.name;
	}

	public void setName(final String name) {
		this.name = name;
	}

	public String getPassword() {
		return this.password;
	}

	public void setPassword(final String password) {
		this.password = password;
	}

	public boolean isAdmin() {
		return this.admin;
	}

	public void setAdmin(final boolean admin) {
		this.admin = admin;
	}

	public String getReadFrom() {
		return this.readFrom;
	}

	public void setReadFrom(final String readFrom) {
		this.readFrom = readFrom;
	}

	public String getWriteTo() {
		return this.writeTo;
	}

	public void setWriteTo(final String writeTo) {
		this.writeTo = writeTo;
	}

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
}