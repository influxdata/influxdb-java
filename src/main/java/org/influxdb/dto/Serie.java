package org.influxdb.dto;

public class Serie {
	private String name;
	private String[] columns;
	private Object[][] points;

	public String getName() {
		return this.name;
	}

	public void setName(final String name) {
		this.name = name;
	}

	public String[] getColumns() {
		return this.columns;
	}

	public void setColumns(final String[] columns) {
		this.columns = columns;
	}

	public Object[][] getPoints() {
		return this.points;
	}

	public void setPoints(final Object[][] points) {
		this.points = points;
	}

}