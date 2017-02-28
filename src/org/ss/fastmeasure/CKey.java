package org.ss.fastmeasure;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class CKey implements WritableComparable<CKey> {
	public VIntWritable gid;
	public ByteWritable flag;

	static {
		WritableComparator.define(CKey.class, new CKeyComparator());
		WritableComparator.define(CKey.class, new CKeyGroupingComparator());
	}

	public CKey() {
		gid = new VIntWritable();
		flag = new ByteWritable();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		if (null != out) {
			gid.write(out);
			flag.write(out);
		}
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		if (null != in) {
			gid.readFields(in);
			flag.readFields(in);
		}
	}

	@Override
	public int compareTo(CKey o) {
		int cmp = gid.compareTo(o.gid);
		if (0 != cmp)
			return cmp;

		return flag.compareTo(o.flag);
	}

	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof CKey))
			return false;
		CKey c = (CKey) obj;
		return gid.equals(c.gid) && flag.equals(c.flag);
	}

	public void set(int gid, byte flag) {
		this.gid.set(gid);
		this.flag.set(flag);
	}

}
