package com.jpmorgan.hbase.mapreduce;

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
import org.apache.hadoop.hbase.client.Delete;
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
import org.apache.zookeeper.KeeperException;

import com.jpmorgan.hbase.row.rename.RowKeyRename;

/**
 * 
 * @author krishdey
 *
 */
public class RowKeyHashImporter extends TableMapper<ImmutableBytesWritable, Mutation> {
	private static final Log LOG = LogFactory.getLog(RowKeyHashImporter.class);
	public final static String WAL_DURABILITY = "import.wal.durability";
	public final static String ROWKEY_HASH_IMPL = "row.key.hash";
	private List<UUID> clusterIds;
	private Durability durability;
	private RowKeyRename rowkeyRenameAlgo;

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
		Delete delete = null;
		if (LOG.isTraceEnabled()) {
			LOG.trace("Considering the row." + Bytes.toString(key.get(), key.getOffset(), key.getLength()));
		}
		processKV(key, result, context, put, delete);
	}

	protected void processKV(ImmutableBytesWritable key, Result result, Context context, Put put, Delete delete)
			throws IOException, InterruptedException {
		LOG.info("Renaming the row " + key.toString());
		ImmutableBytesWritable renameRowKey = rowkeyRenameAlgo.rowKeyRename(key);
		for (Cell kv : result.rawCells()) {
			if (put == null) {
				put = new Put(renameRowKey.get());
			}

			Cell hashedKV = convertKv(kv, renameRowKey);
			addPutToKv(put, hashedKV);

			if (put != null) {
				if (durability != null) {
					put.setDurability(durability);
				}
				put.setClusterIds(clusterIds);
				context.write(key, put);
			}
		}
	}

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

	protected void addPutToKv(Put put, Cell kv) throws IOException {
		put.add(kv);
	}

	@Override
	@SuppressWarnings("unchecked")
	public void setup(Context context) {
		Configuration conf = context.getConfiguration();
		String durabilityStr = conf.get(WAL_DURABILITY);
		String hashRowKey = conf.get(ROWKEY_HASH_IMPL, "com.jpmorgan.hbase.row.rename.DefaultRowKeyRenameImpl");

		if (durabilityStr != null) {
			durability = Durability.valueOf(durabilityStr.toUpperCase(Locale.ROOT));
		}
		// TODO: This is kind of ugly doing setup of ZKW just to read the clusterid.
		ZooKeeperWatcher zkw = null;
		Exception ex = null;
		try {
			zkw = new ZooKeeperWatcher(conf, context.getTaskAttemptID().toString(), null);
			clusterIds = Collections.singletonList(ZKClusterId.getUUIDForCluster(zkw));
			//This is for rowkey rename
			Class<RowKeyRename> hashRowKeyClass = (Class<RowKeyRename>) Class.forName(hashRowKey);
			rowkeyRenameAlgo = ReflectionUtils.newInstance(hashRowKeyClass);
			
		} catch (ZooKeeperConnectionException e) {
			ex = e;
			LOG.error("Problem connecting to ZooKeper during task setup", e);
		} catch (KeeperException e) {
			ex = e;
			LOG.error("Problem reading ZooKeeper data during task setup", e);
		} catch (IOException e) {
			ex = e;
			LOG.error("Problem setting up task", e);
		} catch (ClassNotFoundException e) {
			LOG.error("Problem finding the row key hashing class ", e);
			throw new RuntimeException(e);
		} finally {
			if (zkw != null)
				zkw.close();
		}
		if (clusterIds == null) {
			// exit early if setup fails
			throw new RuntimeException(ex);
		}
	}
}