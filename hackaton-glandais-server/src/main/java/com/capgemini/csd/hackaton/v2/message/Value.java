package com.capgemini.csd.hackaton.v2.message;

public class Value {

	private int sensorId;

	private long value;

	public Value(int sensorId, long value) {
		super();
		this.sensorId = sensorId;
		this.value = value;
	}

	public int getSensorId() {
		return sensorId;
	}

	public long getValue() {
		return value;
	}

	@Override
	public String toString() {
		return "Value [sensorId=" + sensorId + ", value=" + value + "]";
	}

}
