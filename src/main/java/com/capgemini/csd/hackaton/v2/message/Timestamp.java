package com.capgemini.csd.hackaton.v2.message;

public class Timestamp implements Comparable<Timestamp> {

	private long timestamp;

	private int id;

	public Timestamp(long timestamp, int id) {
		super();
		this.timestamp = timestamp;
		this.id = id;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public int getId() {
		return id;
	}

	@Override
	public int compareTo(Timestamp o) {
		int res = Long.compare(timestamp, o.timestamp);
		if (res == 0) {
			return Long.compare(id, o.id);
		} else {
			return res;
		}
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (id ^ (id >>> 32));
		result = prime * result + (int) (timestamp ^ (timestamp >>> 32));
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Timestamp other = (Timestamp) obj;
		if (id != other.id)
			return false;
		if (timestamp != other.timestamp)
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "Timestamp [timestamp=" + timestamp + ", id=" + id + "]";
	}

}
