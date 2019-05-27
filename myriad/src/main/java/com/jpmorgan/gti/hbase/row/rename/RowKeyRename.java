package com.jpmorgan.gti.hbase.row.rename;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

public interface RowKeyRename {
	
	public ImmutableBytesWritable rowKeyRename(ImmutableBytesWritable key);
}
