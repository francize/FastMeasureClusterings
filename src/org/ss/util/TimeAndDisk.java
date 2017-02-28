package org.ss.util;

public class TimeAndDisk {
	public double time;
	public double disk;

	public TimeAndDisk(double time, double disk) {
		this.time = time;
		this.disk = disk;
	}

	public TimeAndDisk() {
	}

	public double getTime() {
		return time;
	}

	public void setTime(double time) {
		this.time = time;
	}

	public double getDisk() {
		return disk;
	}

	public void setDisk(double disk) {
		this.disk = disk;
	}

}
