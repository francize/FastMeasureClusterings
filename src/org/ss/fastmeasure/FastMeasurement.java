package org.ss.fastmeasure;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.ss.util.FileWritten;

public class FastMeasurement {

	static enum DisRecords {
		NODES, SUM_OF_INCONSISTENCY
	}

	public static void main(String[] args) throws Exception {
		if (args.length != 5) {
			System.err.println("Usage: hadoop jar FastMeasurement.jar <in> <out> <reduce_num> <g2> <g3>");
			System.exit(2);
		}
		Configuration conf = new Configuration();

		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		conf.set("g2", otherArgs[3]);
		conf.set("g3", otherArgs[4]);
		System.out.println("**************** Grouping and Counting Inconsistency***************");
		long start = System.currentTimeMillis();
		Job job = Job.getInstance(conf, "Grouping and Counting Inconsistency");
		job.setJarByClass(FastMeasurement.class);
		job.setMapperClass(GroupingMapper.class);
		job.setReducerClass(CountingReducer.class);
		job.setMapOutputKeyClass(CKey.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(WholeFileInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setSortComparatorClass(CKeyComparator.class);
		job.setPartitionerClass(CKeyPartitioner.class);
		job.setGroupingComparatorClass(CKeyGroupingComparator.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		job.setNumReduceTasks(Integer.valueOf(otherArgs[2]));
		job.waitForCompletion(true);

		Counters counters = job.getCounters();
		long num = counters.findCounter(DisRecords.NODES).getValue();
		long sum_of_inconsistency = counters.findCounter(DisRecords.SUM_OF_INCONSISTENCY).getValue();
		double inconsistency_rate = (double) sum_of_inconsistency / (num * (num - 1));
		long end = System.currentTimeMillis();

		System.out.println("The inconsistency percentage is :" + inconsistency_rate);
		System.out.println("The similarity between two graphs is  :" + (1.0 - inconsistency_rate));
		System.out.println("程序运行时间： " + (end - start) * 1.0 / 1000 + "s.");
		double jobWrittenGB = FileWritten.getWrittenGB(job);
		System.out.println("FILE_BYTES_WRITTEN : " + jobWrittenGB + " GB");
		System.exit(1);
	}
}
