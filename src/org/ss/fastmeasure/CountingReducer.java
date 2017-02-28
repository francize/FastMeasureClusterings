package org.ss.fastmeasure;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.StringTokenizer;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.ss.fastmeasure.FastMeasurement.DisRecords;

public class CountingReducer extends Reducer<CKey, Text, NullWritable, Text> {

	@Override
	protected void reduce(CKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		int gid = key.gid.get();
		int sum = 0;
		HashSet<Long> wholeSet = new HashSet<>();
		if (0 != gid) {
			HashMap<Integer, HashSet<Long>> fIndex2Community = new HashMap<>();
			HashMap<Long, Integer> fNode2Index = new HashMap<>();
			int k = 0;
			for (Text text : values) {
				HashSet<Long> community = new HashSet<>();
				HashSet<Long> nodeSet = new HashSet<>();
				StringTokenizer tokenizer = new StringTokenizer(text.toString());
				while (tokenizer.hasMoreTokens()) {
					String pair = tokenizer.nextToken();
					String[] tokens = pair.split(":");
					long v = Long.valueOf(tokens[0]);
					int g = Integer.valueOf(tokens[1]);
					if (g == gid) {
						nodeSet.add(v);
						wholeSet.add(v);
					}
					community.add(v);
				}
				if (0 == key.flag.get()) {
					fIndex2Community.put(k, community);
					for (long v : nodeSet) {
						fNode2Index.put(v, k);
					}
					k++;
				} else if (1 == key.flag.get()) {
					HashMap<Integer, Integer> comparedIndexDiff = new HashMap<>();
					for (long v : nodeSet) {
						int fIndex = fNode2Index.get(v);
						if (comparedIndexDiff.containsKey(fIndex)) {
							sum += comparedIndexDiff.get(fIndex);
						} else {
							HashSet<Long> fpCommunity = fIndex2Community.get(fIndex);
							HashSet<Long> inter = new HashSet<>();
							inter.addAll(fpCommunity);
							inter.retainAll(community);
							int diff = fpCommunity.size() + community.size() - 2 * inter.size();
							comparedIndexDiff.put(fIndex, diff);
							sum += diff;
						}
					}
				}
			}
			context.getCounter(DisRecords.NODES).increment(wholeSet.size());
		} else {
			int total = 0;
			int dif = 0;
			for (Text text : values) {
				StringTokenizer tokenizer = new StringTokenizer(text.toString());
				while (tokenizer.hasMoreTokens()) {
					++total;
					String pair = tokenizer.nextToken();
					String[] tokens = pair.split(":");
					int g = Integer.valueOf(tokens[1]);
					if (g == gid)
						++dif;
				}
			}
			sum = dif * (total - dif) >> 1;
			context.getCounter(DisRecords.NODES).increment(dif >> 1);
		}
		context.write(null, new Text(gid + ":" + sum));
		context.getCounter(DisRecords.SUM_OF_INCONSISTENCY).increment(sum);
	}
}
