package com.jpmorgan.gti.hbase.row.rename;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;

public class DefaultRowKeyNodeRenameImpl implements RowKeyRename {

	@Override
	public ImmutableBytesWritable rowKeyRename(ImmutableBytesWritable... key) {
		String haString = MD5Util.calcMD5SaltedTail(Bytes.toString(key[0].get()));
		return new ImmutableBytesWritable(haString.getBytes());
	}

}
