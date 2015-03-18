package org.influxdb.dto;

import java.util.Iterator;

import org.influxdb.dto.Serie.Row;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Tests for a Serie.
 * 
 * @author stefan . majer [at] gmail.com
 * 
 */
public class SerieTest {

	/**
	 * Test the SerieBuilder.
	 */
	@Test
	public void serieBuilderTest() {
		Serie serie = new Serie.Builder("builder")
				.columns("time", "idle", "error")
				.values(1l, 96.3f, "no error")
				.values(2l, 69.5f, "with error")
				.build();

		Assert.assertNotNull(serie.getColumns());
		Assert.assertEquals(serie.getColumns().length, 3);
		Assert.assertEquals(serie.getColumns()[0], "time");
		Assert.assertEquals(serie.getColumns()[1], "idle");
		Assert.assertEquals(serie.getColumns()[2], "error");

		Assert.assertNotNull(serie.getRows());
		Assert.assertEquals(serie.getRows().size(), 2);
		Assert.assertEquals(serie.getRows().get(0).size(), 3);

		// test iterator
		Iterator<Row> iterator = serie.rows();

		Assert.assertTrue(iterator.hasNext());
		Row first = iterator.next();
		Assert.assertEquals(first.getColumn(0), 1l);
		Assert.assertEquals(first.getColumn(1), 96.3f);
		Assert.assertEquals(first.getColumn(2), "no error");

		Assert.assertTrue(iterator.hasNext());
		Row second = iterator.next();
		Assert.assertEquals(second.getColumn("time"), 2l);
		Assert.assertEquals(second.getColumn("idle"), 69.5f);
		Assert.assertEquals(second.getColumn("error"), "with error");

		Assert.assertFalse(iterator.hasNext());
	}
}
