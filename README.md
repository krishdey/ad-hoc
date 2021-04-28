# ad-hoc
krish

package com.jpmc.gti.paas.sqoop.manager;

import java.io.IOException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.sqoop.SqoopOptions;
import org.apache.sqoop.SqoopOptions.UpdateMode;
import org.apache.sqoop.manager.GenericJdbcManager;
import org.apache.sqoop.mapreduce.ExportBatchOutputFormat;
import org.apache.sqoop.mapreduce.JdbcExportJob;
import org.apache.sqoop.mapreduce.JdbcUpsertExportJob;
import org.apache.sqoop.mapreduce.OracleUpsertOutputFormat;
import org.apache.sqoop.mapreduce.db.OracleDataDrivenDBInputFormat;
import org.apache.sqoop.util.ExportException;
import org.apache.sqoop.util.ImportException;

import com.jpmc.gti.paas.sqoop.util.CacheUtil;

/**
 * Manages connections to GOS Oracle databases. Requires the Oracle JDBC driver.
 */
public class GOSKrbConnManager extends GenericJdbcManager {

	public static final Log LOG = LogFactory.getLog(GOSKrbConnManager.class.getName());

	/**
	 * ORA-00942: Table or view does not exist. Indicates that the user does not
	 * have permissions.
	 */
	public static final int ERROR_TABLE_OR_VIEW_DOES_NOT_EXIST = 942;

	/**
	 * This is a catalog view query to list the databases. For Oracle we map the
	 * concept of a database to a schema, and a schema is identified by a user. In
	 * order for the catalog view DBA_USERS be visible to the user who executes this
	 * query, they must have the DBA privilege.
	 */
	public static final String QUERY_LIST_DATABASES = "SELECT USERNAME FROM DBA_USERS";

	/**
	 * Query to list all tables visible to the current user. Note that this list
	 * does not identify the table owners which is required in order to ensure that
	 * the table can be operated on for import/export purposes.
	 */
	public static final String QUERY_LIST_TABLES = "SELECT TABLE_NAME FROM ALL_TABLES";

	/**
	 * Query to list all columns of the given table. Even if the user has the
	 * privileges to access table objects from another schema, this query will limit
	 * it to explore tables only from within the active schema.
	 */
	public static final String QUERY_COLUMNS_FOR_TABLE = "SELECT COLUMN_NAME FROM ALL_TAB_COLUMNS WHERE "
			+ "OWNER = ? AND TABLE_NAME = ? ORDER BY COLUMN_ID";

	/**
	 * Query to find the primary key column name for a given table. This query is
	 * restricted to the current schema.
	 */
	public static final String QUERY_PRIMARY_KEY_FOR_TABLE = "SELECT ALL_CONS_COLUMNS.COLUMN_NAME FROM ALL_CONS_COLUMNS, "
			+ "ALL_CONSTRAINTS WHERE ALL_CONS_COLUMNS.CONSTRAINT_NAME = " + "ALL_CONSTRAINTS.CONSTRAINT_NAME AND "
			+ "ALL_CONSTRAINTS.CONSTRAINT_TYPE = 'P' AND " + "ALL_CONS_COLUMNS.TABLE_NAME = ? AND "
			+ "ALL_CONS_COLUMNS.OWNER = ?";

	// driver class to ensure is loaded when making db connection.
	private static final String DRIVER_CLASS = "com.jpmc.gti.paas.sqoop.driver.GOSKerberosDriver";

	// Configuration key to use to set the session timezone.
	public static final String ORACLE_TIMEZONE_KEY = "oracle.sessionTimeZone";

	public GOSKrbConnManager(final SqoopOptions opts) throws Exception  {
		super(DRIVER_CLASS, opts);
		this.options.setUsername(CacheUtil.getUsername());
		
		try {
			Class.forName(DRIVER_CLASS);
		} catch (ClassNotFoundException e) {
			throw new RuntimeException("The driver class not found in classpath " + DRIVER_CLASS);
		}
	}

	// Oracle XE does a poor job of releasing server-side resources for
	// closed connections. So we actually want to cache connections as
	// much as possible. This is especially important for JUnit tests which
	// may need to make 60 or more connections (serially), since each test
	// uses a different OracleManager instance.
	private static class ConnCache {

		public static final Log LOG = LogFactory.getLog(ConnCache.class.getName());

		private static class CacheKey {
			private final String connectString;
			private final String username;

			public CacheKey(String connect, String user) {
				this.connectString = connect;
				this.username = user; // note: may be null.
			}

			@Override
			public boolean equals(Object o) {
				if (o instanceof CacheKey) {
					CacheKey k = (CacheKey) o;
					if (null == username) {
						return k.username == null && k.connectString.equals(connectString);
					} else {
						return k.username.equals(username) && k.connectString.equals(connectString);
					}
				} else {
					return false;
				}
			}

			@Override
			public int hashCode() {
				if (null == username) {
					return connectString.hashCode();
				} else {
					return username.hashCode() ^ connectString.hashCode();
				}
			}

			@Override
			public String toString() {
				return connectString + "/" + username;
			}
		}

		private Map<CacheKey, Connection> connectionMap;

		public ConnCache() {
			LOG.debug("Instantiated new connection cache.");
			connectionMap = new HashMap<CacheKey, Connection>();
		}

		/**
		 * @return a Connection instance that can be used to connect to the given
		 *         database, if a previously-opened connection is available in the
		 *         cache. Returns null if none is available in the map.
		 */
		public synchronized Connection getConnection(String connectStr, String username) throws SQLException {
			CacheKey key = new CacheKey(connectStr, username);
			Connection cached = connectionMap.get(key);
			if (null != cached) {
				connectionMap.remove(key);
				if (cached.isReadOnly()) {
					// Read-only mode? Don't want it.
					cached.close();
				}

				if (cached.isClosed()) {
					// This connection isn't usable.
					return null;
				}

				cached.rollback(); // Reset any transaction state.
				cached.clearWarnings();

				LOG.debug("Got cached connection for " + key);
			}

			return cached;
		}

		/**
		 * Returns a connection to the cache pool for future use. If a connection is
		 * already cached for the connectstring/username pair, then this connection is
		 * closed and discarded.
		 */
		public synchronized void recycle(String connectStr, String username, Connection conn) throws SQLException {

			CacheKey key = new CacheKey(connectStr, username);
			Connection existing = connectionMap.get(key);
			if (null != existing) {
				// Cache is already full for this entry.
				LOG.debug("Discarding additional connection for " + key);
				conn.close();
				return;
			}

			// Put it in the map for later use.
			LOG.debug("Caching released connection for " + key);
			connectionMap.put(key, conn);
		}

		@Override
		protected synchronized void finalize() throws Throwable {
			for (Connection c : connectionMap.values()) {
				c.close();
			}

			super.finalize();
		}
	}

	private static final ConnCache CACHE;
	static {
		CACHE = new ConnCache();
	}

	public void close() throws SQLException {
		release(); // Release any open statements associated with the connection.
		if (hasOpenConnection()) {
			// Release our open connection back to the cache.
			CACHE.recycle(options.getConnectString(), options.getUsername(), getConnection());
			discardConnection(false);
		}
	}

	protected String getColNamesQuery(String tableName) {
		// SqlManager uses "tableName AS t" which doesn't work in Oracle.
		String query = "SELECT t.* FROM " + escapeTableName(tableName) + " t WHERE 1=0";

		LOG.debug("Using column names query: " + query);
		return query;
	}

	/**
	 * Create a connection to the database; usually used only from within
	 * getConnection(), which enforces a singleton guarantee around the Connection
	 * object.
	 * 
	 * Oracle-specific driver uses READ_COMMITTED which is the weakest semantics
	 * Oracle supports.
	 */
	protected Connection makeConnection() throws SQLException {

		Connection connection;
		String driverClass = getDriverClass();

		try {
			Class.forName(driverClass);
		} catch (ClassNotFoundException cnfe) {
			throw new RuntimeException("Could not load db driver class: " + driverClass);
		}

	    String username = options.getUsername(); 
	    String connectStr = options.getConnectString(); 
	    
	    connection = CACHE.getConnection(connectStr, username); 
		Properties connectionParams = options.getConnectionParams();
		System.out.println("Make connection to GOS: ");
		connection = DriverManager.getConnection(connectStr, connectionParams);
		System.out.println("Trying connection v1.5 " + connection);
		// We only use this for metadata queries. Loosest semantics are okay.
		connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);

		// Setting session time zone
		setSessionTimeZone(connection);

		return connection;
	}

	/**
	 * Set session time zone.
	 * 
	 * @param conn Connection object
	 * @throws SQLException instance
	 */
	private void setSessionTimeZone(Connection conn) throws SQLException {
		// Need to use reflection to call the method setSessionTimeZone on the
		// OracleConnection class because oracle specific java libraries are not
		// accessible in this context.
		Method method;
		try {
			method = conn.getClass().getMethod("setSessionTimeZone", new Class[] { String.class });
		} catch (Exception ex) {
			LOG.error("Could not find method setSessionTimeZone in " + conn.getClass().getName(), ex);
			// rethrow SQLException
			throw new SQLException(ex);
		}

		// Need to set the time zone in order for Java to correctly access the
		// column "TIMESTAMP WITH LOCAL TIME ZONE". The user may have set this in
		// the configuration as 'oracle.sessionTimeZone'.
		String clientTimeZoneStr = options.getConf().get(ORACLE_TIMEZONE_KEY, "GMT");
		try {
			method.setAccessible(true);
			method.invoke(conn, clientTimeZoneStr);
			LOG.info("Time zone has been set to " + clientTimeZoneStr);
		} catch (Exception ex) {
			LOG.warn("Time zone " + clientTimeZoneStr + " could not be set on Oracle database.");
			LOG.info("Setting default time zone: GMT");
			try {
				// Per the documentation at:
				// http://download-west.oracle.com/docs/cd/B19306_01
				// /server.102/b14225/applocaledata.htm#i637736
				// The "GMT" timezone is guaranteed to exist in the available timezone
				// regions, whereas others (e.g., "UTC") are not.
				method.invoke(conn, "GMT");
			} catch (Exception ex2) {
				LOG.error("Could not set time zone for oracle connection", ex2);
				// rethrow SQLException
				throw new SQLException(ex);
			}
		}
	}

	@Override
	public void importTable(org.apache.sqoop.manager.ImportJobContext context) throws IOException, ImportException {
		context.setConnManager(this);
		// Specify the Oracle-specific DBInputFormat for import.
		context.setInputFormat(OracleDataDrivenDBInputFormat.class);
		super.importTable(context);
	}

	/**
	 * Export data stored in HDFS into a table in a database.
	 */
	public void exportTable(org.apache.sqoop.manager.ExportJobContext context) throws IOException, ExportException {
		context.setConnManager(this);
		JdbcExportJob exportJob = new JdbcExportJob(context, null, null, ExportBatchOutputFormat.class, null);
		exportJob.runExport();
	}

	@Override
	/**
	 * {@inheritDoc}
	 */
	public void upsertTable(org.apache.sqoop.manager.ExportJobContext context) throws IOException, ExportException {
		context.setConnManager(this);
		JdbcUpsertExportJob exportJob = new JdbcUpsertExportJob(context, OracleUpsertOutputFormat.class, null);
		exportJob.runExport();
	}

	@Override
	/**
	 * {@inheritDoc}
	 */
	public void configureDbOutputColumns(SqoopOptions options) {
		if (options.getUpdateMode() == UpdateMode.UpdateOnly) {
			super.configureDbOutputColumns(options);
		} else {
			// We're in upsert mode. We need to explicitly set
			// the database output column ordering in the codeGenerator.
			Set<String> updateKeys = new LinkedHashSet<String>();
			Set<String> updateKeysUppercase = new HashSet<String>();
			String updateKeyValue = options.getUpdateKeyCol();
			StringTokenizer stok = new StringTokenizer(updateKeyValue, ",");
			while (stok.hasMoreTokens()) {
				String nextUpdateColumn = stok.nextToken().trim();
				if (nextUpdateColumn.length() > 0) {
					updateKeys.add(nextUpdateColumn);
					updateKeysUppercase.add(nextUpdateColumn.toUpperCase());
				} else {
					throw new RuntimeException(
							"Invalid update key column value specified" + ": '" + updateKeyValue + "'");
				}
			}

			String[] allColNames = getColumnNames(options.getTableName());
			List<String> dbOutCols = new ArrayList<String>();
			dbOutCols.addAll(updateKeys);
			for (String col : allColNames) {
				if (!updateKeysUppercase.contains(col.toUpperCase())) {
					dbOutCols.add(col); // add update columns to the output order list.
				}
			}
			for (String col : allColNames) {
				dbOutCols.add(col); // add insert columns to the output order list.
			}
			options.setDbOutputColumns(dbOutCols.toArray(new String[dbOutCols.size()]));
		}
	}

	@Override
	public ResultSet readTable(String tableName, String[] columns) throws SQLException {
		if (columns == null) {
			columns = getColumnNames(tableName);
		}

		StringBuilder sb = new StringBuilder();
		sb.append("SELECT ");
		boolean first = true;
		for (String col : columns) {
			if (!first) {
				sb.append(", ");
			}
			sb.append(escapeColName(col));
			first = false;
		}
		sb.append(" FROM ");
		sb.append(escapeTableName(tableName));

		String sqlCmd = sb.toString();
		LOG.debug("Reading table with command: " + sqlCmd);
		return execute(sqlCmd);
	}

	/**
	 * Resolve a database-specific type to the Java type that should contain it.
	 * 
	 * @param tableName  table name
	 * @param columnName column name
	 * @param sqlType    sql data type
	 * @return the name of a Java type to hold the sql datatype, or null if none.
	 */
	@Override
	public String toJavaType(String tableName, String columnName, int sqlType) {
		String defaultJavaType = super.toJavaType(tableName, columnName, sqlType);
		return (defaultJavaType == null) ? dbToJavaType(sqlType) : defaultJavaType;
	}

	/**
	 * Attempt to map sql type to java type.
	 * 
	 * @param sqlType sql type
	 * @return java type
	 */
	private String dbToJavaType(int sqlType) {
		// load class oracle.jdbc.OracleTypes
		// need to use reflection because oracle specific libraries
		// are not accessible in this context
		Class typeClass = getTypeClass("oracle.jdbc.OracleTypes");

		// check if it is TIMESTAMPTZ
		int dbType = getDatabaseType(typeClass, "TIMESTAMPTZ");
		if (sqlType == dbType) {
			return "java.sql.Timestamp";
		}

		// check if it is TIMESTAMPLTZ
		dbType = getDatabaseType(typeClass, "TIMESTAMPLTZ");
		if (sqlType == dbType) {
			return "java.sql.Timestamp";
		}

		// return null if no java type was found for sqlType
		return null;
	}

	/**
	 * Attempt to map sql type to hive type.
	 * 
	 * @param tableName  table name
	 * @param columnName column name
	 * @param sqlType    sql data type
	 * @return hive data type
	 */
	public String toHiveType(String tableName, String columnName, int sqlType) {
		String defaultHiveType = super.toHiveType(tableName, columnName, sqlType);
		return (defaultHiveType == null) ? dbToHiveType(sqlType) : defaultHiveType;
	}

	/**
	 * Resolve a database-specific type to Hive type.
	 * 
	 * @param sqlType sql type
	 * @return hive type
	 */
	private String dbToHiveType(int sqlType) {
		// load class oracle.jdbc.OracleTypes
		// need to use reflection because oracle specific libraries
		// are not accessible in this context
		Class typeClass = getTypeClass("oracle.jdbc.OracleTypes");

		// check if it is TIMESTAMPTZ
		int dbType = getDatabaseType(typeClass, "TIMESTAMPTZ");
		if (sqlType == dbType) {
			return "STRING";
		}

		// check if it is TIMESTAMPLTZ
		dbType = getDatabaseType(typeClass, "TIMESTAMPLTZ");
		if (sqlType == dbType) {
			return "STRING";
		}

		// return null if no hive type was found for sqlType
		return null;
	}

	/**
	 * Get database type.
	 * 
	 * @param clazz     oracle class representing sql types
	 * @param fieldName field name
	 * @return value of database type constant
	 */
	private int getDatabaseType(Class clazz, String fieldName) {
		// Need to use reflection to extract constant values because the database
		// specific java libraries are not accessible in this context.
		int value = -1;
		try {
			java.lang.reflect.Field field = clazz.getDeclaredField(fieldName);
			value = field.getInt(null);
		} catch (NoSuchFieldException ex) {
			LOG.error("Could not retrieve value for field " + fieldName, ex);
		} catch (IllegalAccessException ex) {
			LOG.error("Could not retrieve value for field " + fieldName, ex);
		}
		return value;
	}

	/**
	 * Load class by name.
	 * 
	 * @param className class name
	 * @return class instance
	 */
	private Class getTypeClass(String className) {
		// Need to use reflection to load class because the database specific java
		// libraries are not accessible in this context.
		Class typeClass = null;
		try {
			typeClass = Class.forName(className);
		} catch (ClassNotFoundException ex) {
			LOG.error("Could not load class " + className, ex);
		}
		return typeClass;
	}

	@Override
	protected void finalize() throws Throwable {
		close();
		super.finalize();
	}

	@Override
	protected String getCurTimestampQuery() {
		return "SELECT SYSDATE FROM dual";
	}

	@Override
	public String timestampToQueryString(Timestamp ts) {
		return "TO_TIMESTAMP('" + ts + "', 'YYYY-MM-DD HH24:MI:SS.FF')";
	}

	@Override
	public String datetimeToQueryString(String datetime, int columnType) {
		if (columnType == Types.TIMESTAMP) {
			return "TO_TIMESTAMP('" + datetime + "', 'YYYY-MM-DD HH24:MI:SS.FF')";
		} else if (columnType == Types.DATE) {
			return "TO_DATE('" + datetime + "', 'YYYY-MM-DD HH24:MI:SS')";
		} else {
			String msg = "Column type is neither timestamp nor date!";
			LOG.error(msg);
			throw new RuntimeException(msg);
		}
	}

	@Override
	public boolean supportsStagingForExport() {
		return true;
	}

	/**
	 * The concept of database in Oracle is mapped to schemas. Each schema is
	 * identified by the corresponding username.
	 */
	@Override
	public String[] listDatabases() {
		Connection conn = null;
		Statement stmt = null;
		ResultSet rset = null;
		List<String> databases = new ArrayList<String>();

		try {
			conn = getConnection();
			stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			rset = stmt.executeQuery(QUERY_LIST_DATABASES);

			while (rset.next()) {
				databases.add(rset.getString(1));
			}
			conn.commit();
		} catch (SQLException e) {
			try {
				conn.rollback();
			} catch (Exception ex) {
				LOG.error("Failed to rollback transaction", ex);
			}

			if (e.getErrorCode() == ERROR_TABLE_OR_VIEW_DOES_NOT_EXIST) {
				LOG.error("The catalog view DBA_USERS was not found. "
						+ "This may happen if the user does not have DBA privileges. "
						+ "Please check privileges and try again.");
				LOG.debug("Full trace for ORA-00942 exception", e);
			} else {
				LOG.error("Failed to list databases", e);
			}
		} finally {
			if (rset != null) {
				try {
					rset.close();
				} catch (SQLException ex) {
					LOG.error("Failed to close resultset", ex);
				}
			}
			if (stmt != null) {
				try {
					stmt.close();
				} catch (Exception ex) {
					LOG.error("Failed to close statement", ex);
				}
			}

			try {
				close();
			} catch (SQLException ex) {
				LOG.error("Unable to discard connection", ex);
			}
		}

		return databases.toArray(new String[databases.size()]);
	}

	@Override
	public String[] listTables() {
		Connection conn = null;
		Statement stmt = null;
		ResultSet rset = null;
		List<String> tables = new ArrayList<String>();

		try {
			conn = getConnection();
			stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			rset = stmt.executeQuery(QUERY_LIST_TABLES);

			while (rset.next()) {
				tables.add(rset.getString(1));
			}
			conn.commit();
		} catch (SQLException e) {
			try {
				conn.rollback();
			} catch (Exception ex) {
				LOG.error("Failed to rollback transaction", ex);
			}
			LOG.error("Failed to list tables", e);
		} finally {
			if (rset != null) {
				try {
					rset.close();
				} catch (SQLException ex) {
					LOG.error("Failed to close resultset", ex);
				}
			}
			if (stmt != null) {
				try {
					stmt.close();
				} catch (Exception ex) {
					LOG.error("Failed to close statement", ex);
				}
			}

			try {
				close();
			} catch (SQLException ex) {
				LOG.error("Unable to discard connection", ex);
			}
		}

		return tables.toArray(new String[tables.size()]);
	}

	@Override
	public String[] getColumnNames(String tableName) {
		Connection conn = null;
		PreparedStatement pStmt = null;
		ResultSet rset = null;
		List<String> columns = new ArrayList<String>();

		String tableOwner = this.options.getUsername();
		String shortTableName = tableName;
		int qualifierIndex = tableName.indexOf('.');
		if (qualifierIndex != -1) {
			tableOwner = tableName.substring(0, qualifierIndex);
			shortTableName = tableName.substring(qualifierIndex + 1);
		}

		try {
			conn = getConnection();

			pStmt = conn.prepareStatement(QUERY_COLUMNS_FOR_TABLE, ResultSet.TYPE_FORWARD_ONLY,
					ResultSet.CONCUR_READ_ONLY);

			pStmt.setString(1, tableOwner);

			pStmt.setString(2, shortTableName);
			rset = pStmt.executeQuery();

			while (rset.next()) {
				columns.add(rset.getString(1));
			}
			conn.commit();
		} catch (SQLException e) {
			try {
				conn.rollback();
			} catch (Exception ex) {
				LOG.error("Failed to rollback transaction", ex);
			}
			LOG.error("Failed to list columns", e);
		} finally {
			if (rset != null) {
				try {
					rset.close();
				} catch (SQLException ex) {
					LOG.error("Failed to close resultset", ex);
				}
			}
			if (pStmt != null) {
				try {
					pStmt.close();
				} catch (Exception ex) {
					LOG.error("Failed to close statement", ex);
				}
			}

			try {
				close();
			} catch (SQLException ex) {
				LOG.error("Unable to discard connection", ex);
			}
		}

		return columns.toArray(new String[columns.size()]);
	}

	@Override
	public String getPrimaryKey(String tableName) {
		Connection conn = null;
		PreparedStatement pStmt = null;
		ResultSet rset = null;
		List<String> columns = new ArrayList<String>();

		String tableOwner = this.options.getUsername();
		String shortTableName = tableName;
		int qualifierIndex = tableName.indexOf('.');
		if (qualifierIndex != -1) {
			tableOwner = tableName.substring(0, qualifierIndex);
			shortTableName = tableName.substring(qualifierIndex + 1);
		}

		try {
			conn = getConnection();

			pStmt = conn.prepareStatement(QUERY_PRIMARY_KEY_FOR_TABLE, ResultSet.TYPE_FORWARD_ONLY,
					ResultSet.CONCUR_READ_ONLY);
			pStmt.setString(1, shortTableName);
			pStmt.setString(2, tableOwner);
			rset = pStmt.executeQuery();

			while (rset.next()) {
				columns.add(rset.getString(1));
			}
			conn.commit();
		} catch (SQLException e) {
			try {
				conn.rollback();
			} catch (Exception ex) {
				LOG.error("Failed to rollback transaction", ex);
			}
			LOG.error("Failed to list columns", e);
		} finally {
			if (rset != null) {
				try {
					rset.close();
				} catch (SQLException ex) {
					LOG.error("Failed to close resultset", ex);
				}
			}
			if (pStmt != null) {
				try {
					pStmt.close();
				} catch (Exception ex) {
					LOG.error("Failed to close statement", ex);
				}
			}

			try {
				close();
			} catch (SQLException ex) {
				LOG.error("Unable to discard connection", ex);
			}
		}

		if (columns.size() == 0) {
			// Table has no primary key
			return null;
		}

		if (columns.size() > 1) {
			// The primary key is multi-column primary key. Warn the user.
			// TODO select the appropriate column instead of the first column based
			// on the datatype - giving preference to numerics over other types.
			LOG.warn("The table " + tableName + " " + "contains a multi-column primary key. Sqoop will default to "
					+ "the column " + columns.get(0) + " only for this job.");
		}

		return columns.get(0);
	}

	@Override
	public String getInputBoundsQuery(String splitByCol, String sanitizedQuery) {
		/*
		 * The default input bounds query generated by DataDrivenImportJob is of the
		 * form: SELECT MIN(splitByCol), MAX(splitByCol) FROM (sanitizedQuery) AS t1
		 * 
		 * This works for most databases but not Oracle since Oracle does not allow the
		 * use of "AS" to project the subquery as a table. Instead the correct format
		 * for use with Oracle is as follows: SELECT MIN(splitByCol), MAX(splitByCol)
		 * FROM (sanitizedQuery) t1
		 */
		return "SELECT MIN(" + splitByCol + "), MAX(" + splitByCol + ") FROM (" + sanitizedQuery + ") t1";
	}
}
