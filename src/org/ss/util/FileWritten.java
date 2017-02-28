package org.ss.util;

import org.apache.hadoop.mapreduce.Job;

public class FileWritten {
	public static final long GB = 1 << 30;

	public static double getWrittenGB(Job job) throws Exception {
		return job.getCounters().findCounter("FileSystemCounters", "FILE_BYTES_WRITTEN").getValue() * 1.0 / GB;
	}
}
