package org.ss.fastmeasure;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.ss.util.ConfigUtils;

public class GroupingMapper extends Mapper<NullWritable, BytesWritable, CKey, Text> {

	private double g2 = 1.0d;
	private double g3 = 1.0d;

	private HashMap<Long, Integer> gMap;

	private CKey outKey;
	private Text outValue;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		g2 = ConfigUtils.getValue(conf, "g2", 1.0d);
		g3 = ConfigUtils.getValue(conf, "g3", 1.0d);
		outKey = new CKey();
		outValue = new Text();
		gMap = new HashMap<>();
	}

	@Override
	protected void map(NullWritable key, BytesWritable value, Context context)
			throws IOException, InterruptedException {
		HashSet<Long> DB = new HashSet<>(); // 存放所有的元素的集合
		HashMap<String, HashSet<Long>> fpCommunities = new HashMap<>(); // 存放簇集Cf
		HashMap<String, HashSet<Long>> mhCommunities = new HashMap<>(); // 存放簇集Cm
		HashSet<Long> Cfmax = new HashSet<>();
		HashSet<Long> Cmmax = new HashSet<>();

		int fpLargeSize = -1;
		int mhLargeSize = -1;

		byte[] tem = value.getBytes();
		String whole = new String(tem, 0, tem.length);
		StringTokenizer st = new StringTokenizer(whole, "\n");
		while (st.hasMoreTokens()) {
			String line = st.nextToken();
			if (null == line || line.isEmpty() || line.equals(""))
				break;
			String elements[] = line.trim().split("\t");
			if (elements.length < 2) {
				elements = line.trim().split("\\s+");
			}
			String cid = elements[0]; // 集合的id
			int len = elements.length;
			if (1 == len) {
				break;
			}
			int size = len - 1;
			if ("f".equals(cid.substring(0, 1))) {
				HashSet<Long> community = new HashSet<>(); // 簇集中的集合
				for (int id = 1; id != len; ++id) {
					community.add(Long.valueOf(elements[id]));
				}
				DB.addAll(community);
				if (size > fpLargeSize) {
					fpLargeSize = size;
					Cfmax.clear();
					Cfmax.addAll(community);
				}
				fpCommunities.put(cid, community);
			} else if ("m".equals(cid.substring(0, 1))) {
				HashSet<Long> community = new HashSet<>(); // 簇集中的集合
				for (int id = 1; id != len; ++id) {
					community.add(Long.valueOf(elements[id]));
				}
				if (size > mhLargeSize) {
					mhLargeSize = size;
					Cmmax.clear();
					Cmmax.addAll(community);
				}
				mhCommunities.put(cid, community);
			}
		}

		HashSet<Long> Ccom = new HashSet<>();
		HashSet<Long> Cunion = new HashSet<>();
		HashSet<Long> Cdiff = new HashSet<>();
		HashSet<Long> Celse = new HashSet<>();

		Ccom.addAll(Cfmax);
		Ccom.retainAll(Cmmax);

		Cunion.addAll(Cfmax);
		Cunion.addAll(Cmmax);

		Cdiff.addAll(Cunion);
		Cdiff.removeAll(Ccom);

		Celse.addAll(DB);
		Celse.removeAll(Cunion);

		Cunion.clear();
		DB.clear();
		for (long u : Ccom) {
			gMap.put(u, 0);
		}
		Ccom.clear();

		int count = 0;
		int nodeNumGroup2 = (int) Math.ceil(Cdiff.size() / g2);
		for (long u : Cdiff) {
			gMap.put(u, 1 + (count++) / nodeNumGroup2);
		}
		Cdiff.clear();

		count = 0;
		int nodeNumGroup3 = (int) Math.ceil(Celse.size() / g3);
		int g2t = (int) g2;
		for (long u : Celse) {
			gMap.put(u, 1 + g2t + (count++) / nodeNumGroup3);
		}
		Celse.clear();

		byte flag = 0;
		for (String cid : fpCommunities.keySet()) {
			HashSet<Long> community = fpCommunities.get(cid);
			emit(context, community, flag);
		}
		
		flag = 1;
		for (String cid : mhCommunities.keySet()) {
			HashSet<Long> community = mhCommunities.get(cid);
			emit(context, community, flag);
		}
	}

	private void emit(Context context, HashSet<Long> community, byte flag) throws IOException, InterruptedException {
		StringBuilder sb = new StringBuilder();
		HashSet<Integer> gidSet = new HashSet<>();
		Iterator<Long> iter = community.iterator();
		for (;;) {
			long v = iter.next();
			int gid = gMap.get(v);
			gidSet.add(gid);
			sb.append(v + ":" + gid);
			if (!iter.hasNext())
				break;
			sb.append(" ");
		}
		outValue.set(sb.toString());
		for (int g : gidSet) {
			outKey.set(g, flag);
			context.write(outKey, outValue);
		}
	}
}