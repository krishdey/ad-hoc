package com.jpmorgan.hbase.row.rename;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

public interface RowKeyRename {
	
	public ImmutableBytesWritable rowKeyRename(ImmutableBytesWritable key);
}
