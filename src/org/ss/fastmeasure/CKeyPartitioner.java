package org.ss.fastmeasure;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class CKeyPartitioner extends Partitioner<CKey, Text> {

	@Override
	public int getPartition(CKey key, Text value, int numPartitions) {
		return key.gid.get() % numPartitions;
	}

}
