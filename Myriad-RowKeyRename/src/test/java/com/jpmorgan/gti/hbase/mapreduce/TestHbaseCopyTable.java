package com.jpmorgan.gti.hbase.mapreduce;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MapReduceTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.LauncherSecurityManager;
import org.apache.hadoop.util.ToolRunner;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import com.jpmorgan.gti.hbase.mapreduce.CopyTable;
import com.jpmorgan.gti.hbase.row.rename.MD5Util;

/**
 * Basic test for the CopyTable M/R tool
 */
@Category({ MapReduceTests.class, LargeTests.class })
public class TestHbaseCopyTable {

	private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
	private static final byte[] ROW1 = Bytes.toBytes("row1");
	private static final byte[] ROW2 = Bytes.toBytes("row2");
	private static final String FAMILY_A_STRING = "a";
	private static final String FAMILY_B_STRING = "b";
	private static final byte[] FAMILY_A = Bytes.toBytes(FAMILY_A_STRING);
	private static final byte[] FAMILY_B = Bytes.toBytes(FAMILY_B_STRING);
	private static final byte[] QUALIFIER = Bytes.toBytes("q");

	@Rule
	public TestName name = new TestName();

	@BeforeClass
	public static void beforeClass() throws Exception {
		TEST_UTIL.startMiniCluster(3);
	}

	@AfterClass
	public static void afterClass() throws Exception {
		TEST_UTIL.shutdownMiniCluster();
	}

	private void doCopyTableTest(boolean bulkload) throws Exception {
		final TableName tableName1 = TableName.valueOf(name.getMethodName() + "1");
		final TableName tableName2 = TableName.valueOf(name.getMethodName() + "2");
		final byte[] FAMILY = Bytes.toBytes("family");
		final byte[] COLUMN1 = Bytes.toBytes("c1");

		try (Table t1 = TEST_UTIL.createTable(tableName1, FAMILY);
				Table t2 = TEST_UTIL.createTable(tableName2, FAMILY)) {
			// put rows into the first table
			for (int i = 0; i < 10; i++) {
				Put p = new Put(Bytes.toBytes("row" + i));
				p.addColumn(FAMILY, COLUMN1, COLUMN1);
				t1.put(p);
			}

			CopyTable copy = new CopyTable(TEST_UTIL.getConfiguration());

			int code;
			if (bulkload) {
				code = ToolRunner.run(new Configuration(TEST_UTIL.getConfiguration()), copy, new String[] {
						"--new.name=" + tableName2.getNameAsString(), "--bulkload", tableName1.getNameAsString() });
			} else {
				code = ToolRunner.run(new Configuration(TEST_UTIL.getConfiguration()), copy,
						new String[] { "--new.name=" + tableName2.getNameAsString(), tableName1.getNameAsString() });
			}
			assertEquals("copy job failed", 0, code);

			// verify the data was copied into table 2
			for (int i = 0; i < 10; i++) {
				Get g = new Get(Bytes.toBytes("row" + i));
				Result r = t2.get(g);
				assertEquals(1, r.size());
				assertTrue(CellUtil.matchingQualifier(r.rawCells()[0], COLUMN1));
			}
		} finally {
			TEST_UTIL.deleteTable(tableName1);
			TEST_UTIL.deleteTable(tableName2);
		}
	}

	/**
	 * Simple end-to-end test
	 * 
	 * @throws Exception
	 */
	@Test
	public void testCopyTable() throws Exception {
		doCopyTableTest(false);
	}

	/**
	 * Simple end-to-end test with bulkload.
	 */
	@Test
	public void testCopyTableWithBulkload() throws Exception {
		doCopyTableTest(true);
	}

	/**
	 * Test copy of table from sourceTable to targetTable all rows from family a
	 */
	@Test
	public void testRenameFamily() throws Exception {
		final TableName sourceTable = TableName.valueOf(name.getMethodName() + "source");
		final TableName targetTable = TableName.valueOf(name.getMethodName() + "-target");

		byte[][] families = { FAMILY_A, FAMILY_B };

		Table t = TEST_UTIL.createTable(sourceTable, families);
		Table t2 = TEST_UTIL.createTable(targetTable, families);
		Put p = new Put(ROW1);
		p.addColumn(FAMILY_A, QUALIFIER, Bytes.toBytes("Data11"));
		p.addColumn(FAMILY_B, QUALIFIER, Bytes.toBytes("Data12"));
		p.addColumn(FAMILY_A, QUALIFIER, Bytes.toBytes("Data13"));
		t.put(p);
		p = new Put(ROW2);
		p.addColumn(FAMILY_B, QUALIFIER, Bytes.toBytes("Dat21"));
		p.addColumn(FAMILY_A, QUALIFIER, Bytes.toBytes("Data22"));
		p.addColumn(FAMILY_B, QUALIFIER, Bytes.toBytes("Data23"));
		t.put(p);

		long currentTime = System.currentTimeMillis();
		String[] args = new String[] { "--new.name=" + targetTable, "--families=a:b", "--all.cells",
				"--starttime=" + (currentTime - 100000), "--endtime=" + (currentTime + 100000), "--versions=1",
				sourceTable.getNameAsString() };
		assertNull(t2.get(new Get(ROW1)).getRow());

		assertTrue(runCopy(args));

		assertNotNull(t2.get(new Get(ROW1)).getRow());
		Result res = t2.get(new Get(ROW1));
		byte[] b1 = res.getValue(FAMILY_B, QUALIFIER);
		assertEquals("Data13", new String(b1));
		assertNotNull(t2.get(new Get(ROW2)).getRow());
		res = t2.get(new Get(ROW2));
		b1 = res.getValue(FAMILY_A, QUALIFIER);
		// Data from the family of B is not copied
		assertNull(b1);

	}

	/**
	 * Test main method of CopyTable.
	 */
	@Test
	public void testMainMethod() throws Exception {
		String[] emptyArgs = { "-h" };
		PrintStream oldWriter = System.err;
		ByteArrayOutputStream data = new ByteArrayOutputStream();
		PrintStream writer = new PrintStream(data);
		System.setErr(writer);
		SecurityManager SECURITY_MANAGER = System.getSecurityManager();
		LauncherSecurityManager newSecurityManager = new LauncherSecurityManager();
		System.setSecurityManager(newSecurityManager);
		try {
			CopyTable.main(emptyArgs);
			fail("should be exit");
		} catch (SecurityException e) {
			assertEquals(1, newSecurityManager.getExitCode());
		} finally {
			System.setErr(oldWriter);
			System.setSecurityManager(SECURITY_MANAGER);
		}
		assertTrue(data.toString().contains("rs.class"));
		// should print usage information
		assertTrue(data.toString().contains("Usage:"));
	}

	private boolean runCopy(String[] args) throws Exception {
		int status = ToolRunner.run(new Configuration(TEST_UTIL.getConfiguration()),
				new CopyTable(TEST_UTIL.getConfiguration()), args);
		return status == 0;
	}

	private static final byte[] FAMILY_NODE = Bytes.toBytes("node");
	private static final byte[] QUALIFIER_NODE = Bytes.toBytes("hashedIdentifier");

	@Test
	public void testCopyTabletWithNodeRowRenamed() throws Exception {
		final TableName tableName1 = TableName.valueOf(name.getMethodName() + "1");
		final TableName tableName2 = TableName.valueOf(name.getMethodName() + "2");

		try (Table t1 = TEST_UTIL.createTable(tableName1, FAMILY_NODE);
				Table t2 = TEST_UTIL.createTable(tableName2, FAMILY_NODE)) {
			// put rows into the first table
			for (int i = 0; i < 10; i++) {
				Put p = new Put(Bytes.toBytes("row" + i));
				p.addColumn(FAMILY_NODE, QUALIFIER_NODE, "krish".getBytes());
				t1.put(p);
			}

			CopyTable copy = new CopyTable(TEST_UTIL.getConfiguration());

			int code;

			code = ToolRunner.run(new Configuration(TEST_UTIL.getConfiguration()), copy, new String[] {
					"--new.name=" + tableName2.getNameAsString(), "--renamerow=node", tableName1.getNameAsString() });

			assertEquals("copy job failed", 0, code);

			// verify the data was copied into table 2
			for (int i = 0; i < 10; i++) {
				Get g = new Get(Bytes.toBytes(MD5Util.calcMD5SaltedTail("row" + i)));
				Result r = t2.get(g);
				assertEquals(1, r.size());
				assertTrue(CellUtil.matchingQualifier(r.rawCells()[0], QUALIFIER_NODE));
			}
		} finally {
			TEST_UTIL.deleteTable(tableName1);
			TEST_UTIL.deleteTable(tableName2);
		}
	}

	private static final byte[] FAMILY_NODELINK = Bytes.toBytes("nodelink");
	private static final byte[] Q_LEFT_NODE_ID = Bytes.toBytes("left_nodeid");
	private static final byte[] Q_LINK_TYPE_ID = Bytes.toBytes("linktypeid");
	private static final byte[] Q_RIGHT_NODE_ID = Bytes.toBytes("right_nodeid");

	@Test
	public void testCopyTabletWithNodeLinkRowRenamed() throws Exception {
		final TableName tableName1 = TableName.valueOf(name.getMethodName() + "1");
		final TableName tableName2 = TableName.valueOf(name.getMethodName() + "2");

		try (Table t1 = TEST_UTIL.createTable(tableName1, FAMILY_NODELINK);
				Table t2 = TEST_UTIL.createTable(tableName2, FAMILY_NODELINK)) {
			// put rows into the first table

			Put p = new Put(Bytes.toBytes("row1"));
			p.addColumn(FAMILY_NODELINK, Q_LEFT_NODE_ID, Bytes.toBytes("krish"));
			p.addColumn(FAMILY_NODELINK, Q_LINK_TYPE_ID, "ramit".getBytes());
			p.addColumn(FAMILY_NODELINK, Q_RIGHT_NODE_ID, "mahi".getBytes());
			t1.put(p);

			CopyTable copy = new CopyTable(TEST_UTIL.getConfiguration());

			int code;

			code = ToolRunner.run(new Configuration(TEST_UTIL.getConfiguration()), copy,
					new String[] { "--new.name=" + tableName2.getNameAsString(), "--renamerow=nodelink",
							tableName1.getNameAsString() });

			assertEquals("copy job failed", 0, code);

			// verify the data was copied into table 2

			Get g = new Get(Bytes.toBytes(MD5Util.calcMD5SaltedTail("krish", "ramit", "mahi")));
			Result r = t2.get(g);
			assertEquals(4, r.size());

		} finally {
			TEST_UTIL.deleteTable(tableName1);
			TEST_UTIL.deleteTable(tableName2);
		}
	}
}
