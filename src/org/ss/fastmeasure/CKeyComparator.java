package org.ss.fastmeasure;

import java.io.IOException;

import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

public class CKeyComparator extends WritableComparator {

	public CKeyComparator() {
		super(CKey.class);
	}

	@Override
	public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
		System.out.println("fuck");
		try {
			int gid1 = readVInt(b1, s1);
			int gid2 = readVInt(b2, s2);
			int cmp = gid1 < gid2 ? -1 : (gid1 == gid2) ? 0 : 1;
			if (0 != cmp)
				return cmp;

			int n2_1 = WritableUtils.decodeVIntSize(b1[s1]);
			int n2_2 = WritableUtils.decodeVIntSize(b2[s2]);
			long flag1 = readVInt(b1, s1 + n2_1);
			long flag2 = readVInt(b2, s2 + n2_2);
			return flag1 < flag2 ? -1 : (flag1 == flag2) ? 0 : 1;
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
	
	@Override
	public int compare(Object a, Object b) {
		System.out.println("shit");
		return super.compare(a, b);
	}
}
