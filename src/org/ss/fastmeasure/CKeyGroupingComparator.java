package org.ss.fastmeasure;

import java.io.IOException;

import org.apache.hadoop.io.WritableComparator;

public class CKeyGroupingComparator extends WritableComparator {

	public CKeyGroupingComparator() {
		super(CKey.class);
	}

	@Override
	public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
		try {
			int gid1 = readVInt(b1, s1);
			int gid2 = readVInt(b2, s2);
			return gid1 < gid2 ? -1 : (gid1 == gid2) ? 0 : 1;
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
}
