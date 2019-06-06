package com.jpmorgan.gti.hbase.mapreduce;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ReflectionUtils;
import org.apache.hadoop.hbase.zookeeper.ZKClusterId;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.zookeeper.KeeperException;

import com.jpmorgan.gti.hbase.row.rename.RowKeyRename;

/**
 * 
 * @author krishdey
 *
 */
public class RowKeyRenameImporter {
	private static final Log LOG = LogFactory.getLog(RowKeyRenameImporter.class);
	public final static String WAL_DURABILITY = "import.wal.durability";
	public final static String ROWKEY_RENAME_IMPL = "row.key.rename";
	private static final byte[] HASH_QUALIFIER = Bytes.toBytes("hashedIdentifier");
	private static final byte[] EMPTY_STRING_BYTES = Bytes.toBytes("");
	private static List<UUID> clusterIds;
	private static Durability durability;

	// helper: create a new KeyValue based on renaming of row Key
	private static Cell convertKv(Cell kv, ImmutableBytesWritable renameRowKey) {
		byte[] newCfName = CellUtil.cloneFamily(kv);

		kv = new KeyValue(renameRowKey.get(), // row buffer
				renameRowKey.getOffset(), // row offset
				renameRowKey.getLength(), // row length
				newCfName, // CF buffer
				0, // CF offset
				kv.getFamilyLength(), // CF length
				kv.getQualifierArray(), // qualifier buffer
				kv.getQualifierOffset(), // qualifier offset
				kv.getQualifierLength(), // qualifier length
				kv.getTimestamp(), // timestamp
				KeyValue.Type.codeToType(kv.getTypeByte()), // KV Type
				kv.getValueArray(), // value buffer
				kv.getValueOffset(), // value offset
				kv.getValueLength()); // value length
		return kv;
	}

	private static void addPutToKv(Put put, Cell kv) throws IOException {
		put.add(kv);
	}

	private static void setDurabilityAnClusterIDs(Configuration conf, Context context) {
		String durabilityStr = conf.get(WAL_DURABILITY);
		if (durabilityStr != null) {
			durability = Durability.valueOf(durabilityStr.toUpperCase(Locale.ROOT));
		}
		// TODO: This is kind of ugly doing setup of ZKW just to read the clusterid.
		ZooKeeperWatcher zkw = null;
		Exception ex = null;
		try {
			zkw = new ZooKeeperWatcher(conf, context.getTaskAttemptID().toString(), null);
			clusterIds = Collections.singletonList(ZKClusterId.getUUIDForCluster(zkw));

		} catch (ZooKeeperConnectionException e) {
			ex = e;
			LOG.error("Problem connecting to ZooKeper during task setup", e);
		} catch (KeeperException e) {
			ex = e;
			LOG.error("Problem reading ZooKeeper data during task setup", e);
		} catch (IOException e) {
			ex = e;
			LOG.error("Problem setting up task", e);
		} finally {
			if (zkw != null)
				zkw.close();
		}
		if (clusterIds == null) {
			// exit early if setup fails
			throw new RuntimeException(ex);
		}

	}

	/**
	 * 
	 * NodeKeyRenameImport
	 *
	 */
	public static class NodeKeyRenameImport extends TableMapper<ImmutableBytesWritable, Mutation> {
		private RowKeyRename rowkeyRenameAlgo;
		private static final byte[] NODE_FAMILY = Bytes.toBytes("node");
		private ImmutableBytesWritable renameRowKey = new ImmutableBytesWritable(EMPTY_STRING_BYTES);

		/**
		 * @param row     The current table row key.
		 * @param value   The columns.
		 * @param context The current context.
		 * @throws IOException When something is broken with the data.
		 */
		@Override
		public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException {
			try {
				writeResult(row, value, context);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		private void writeResult(ImmutableBytesWritable key, Result result, Context context)
				throws IOException, InterruptedException {
			Put put = null;
			if (LOG.isTraceEnabled()) {
				LOG.trace("Considering the row." + Bytes.toString(key.get(), key.getOffset(), key.getLength()));
			}
			processKV(key, result, context, put);
		}

		protected void processKV(ImmutableBytesWritable key, Result result, Context context, Put put)
				throws IOException, InterruptedException {
			if (LOG.isDebugEnabled()) {
				LOG.debug("Renaming the row " + Bytes.toString(key.get()));
			}
			renameRowKey.set(rowkeyRenameAlgo.rowKeyRename(key).get());
			for (Cell kv : result.rawCells()) {
				if (put == null) {
					put = new Put(renameRowKey.get());
					put.addColumn(NODE_FAMILY, HASH_QUALIFIER, renameRowKey.get());
				}

				Cell renamedKV = convertKv(kv, renameRowKey);
				addPutToKv(put, renamedKV);

			}
			if (put != null) {
				if (durability != null) {
					put.setDurability(durability);
				}
				put.setClusterIds(clusterIds);

			}
			context.write(key, put);
			// reset
			renameRowKey.set(EMPTY_STRING_BYTES);
		}

		@Override
		@SuppressWarnings("unchecked")
		public void setup(Context context) {
			Configuration conf = context.getConfiguration();
			String renameRowKey = conf.get(ROWKEY_RENAME_IMPL,
					"com.jpmorgan.gti.hbase.row.rename.DefaultRowKeyNodeRenameImpl");
			try {
				// This is for rowkey rename
				Class<RowKeyRename> renameRowKeyClass = (Class<RowKeyRename>) Class.forName(renameRowKey);
				rowkeyRenameAlgo = ReflectionUtils.newInstance(renameRowKeyClass);
			} catch (ClassNotFoundException e) {
				LOG.error("Problem finding the row key rename class ", e);
				throw new RuntimeException(e);

			}
			setDurabilityAnClusterIDs(conf, context);
		}
	}

	/**
	 * 
	 * NodeLinkRenameImport
	 *
	 */
	public static class NodeLinkRenameImport extends TableMapper<ImmutableBytesWritable, Mutation> {
		private RowKeyRename rowkeyRenameAlgo;
		private static final byte[] NODELINK_FAMILY = Bytes.toBytes("nodelink");
		private static final byte[] Q_LEFT_NODE_ID = Bytes.toBytes("left_nodeid");
		private static final byte[] Q_LINK_TYPE_ID = Bytes.toBytes("linktypeid");
		private static final byte[] Q_RIGHT_NODE_ID = Bytes.toBytes("right_nodeid");

		private ImmutableBytesWritable leftNodeId = new ImmutableBytesWritable(EMPTY_STRING_BYTES);
		private ImmutableBytesWritable linkTypeId = new ImmutableBytesWritable(EMPTY_STRING_BYTES);
		private ImmutableBytesWritable rightNodeId = new ImmutableBytesWritable(EMPTY_STRING_BYTES);
		private ImmutableBytesWritable renameRowKey = new ImmutableBytesWritable(EMPTY_STRING_BYTES);
		private Cell leftNodeCell = null;
		private Cell linkTypeCell = null;
		private Cell rightNodeCell = null;

		/**
		 * @param row     The current table row key.
		 * @param value   The columns.
		 * @param context The current context.
		 * @throws IOException When something is broken with the data.
		 */
		@Override
		public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException {
			try {
				writeResult(row, value, context);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		private void writeResult(ImmutableBytesWritable key, Result result, Context context)
				throws IOException, InterruptedException {
			Put put = null;
			if (LOG.isTraceEnabled()) {
				LOG.trace("Considering the row." + Bytes.toString(key.get(), key.getOffset(), key.getLength()));
			}
			processKV(key, result, context, put);
		}

		protected void processKV(ImmutableBytesWritable key, Result result, Context context, Put put)
				throws IOException, InterruptedException {
			if (LOG.isDebugEnabled()) {
				LOG.debug("Renaming the row " + Bytes.toString(key.get()));
			}
			// Get the left, link and right node
			leftNodeCell = result.getColumnLatestCell(NODELINK_FAMILY, Q_LEFT_NODE_ID);
			if (leftNodeCell != null) {
				leftNodeId.set(CellUtil.cloneValue(leftNodeCell));
			}
			
			linkTypeCell = result.getColumnLatestCell(NODELINK_FAMILY, Q_LINK_TYPE_ID);
			if (linkTypeCell != null) {
				linkTypeId.set(CellUtil.cloneValue(linkTypeCell));
			}

			rightNodeCell = result.getColumnLatestCell(NODELINK_FAMILY, Q_RIGHT_NODE_ID);
			if (rightNodeCell != null) {
				rightNodeId.set(CellUtil.cloneValue(rightNodeCell));
			}

			renameRowKey.set(rowkeyRenameAlgo.rowKeyRename(leftNodeId, linkTypeId, rightNodeId).get());
			for (Cell kv : result.rawCells()) {
				if (put == null) {
					put = new Put(renameRowKey.get());
					put.addColumn(NODELINK_FAMILY, HASH_QUALIFIER, renameRowKey.get());
				}

				Cell renamedKV = convertKv(kv, renameRowKey);
				addPutToKv(put, renamedKV);
			}
			if (put != null) {
				if (durability != null) {
					put.setDurability(durability);
				}
				put.setClusterIds(clusterIds);
			}
			context.write(key, put);
			reset();
		}

		// reset
		private void reset() {
			leftNodeId.set(EMPTY_STRING_BYTES);
			linkTypeId.set(EMPTY_STRING_BYTES);
			rightNodeId.set(EMPTY_STRING_BYTES);
			renameRowKey.set(EMPTY_STRING_BYTES);
			leftNodeCell = null;
			linkTypeCell = null;
			rightNodeCell = null;
		}

		@Override
		@SuppressWarnings("unchecked")
		public void setup(Context context) {
			Configuration conf = context.getConfiguration();

			String renameRowKey = conf.get(ROWKEY_RENAME_IMPL,
					"com.jpmorgan.gti.hbase.row.rename.DefaultRowKeyNodeLinkRenameImpl");
			try {
				// This is for rowkey rename
				Class<RowKeyRename> renameRowKeyClass = (Class<RowKeyRename>) Class.forName(renameRowKey);
				rowkeyRenameAlgo = ReflectionUtils.newInstance(renameRowKeyClass);
			} catch (ClassNotFoundException e) {
				LOG.error("Problem finding the row key rename class ", e);
				throw new RuntimeException(e);
			}
			setDurabilityAnClusterIDs(conf, context);
		}
	}
}