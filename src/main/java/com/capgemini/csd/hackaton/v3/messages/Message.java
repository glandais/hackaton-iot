package com.capgemini.csd.hackaton.v3.messages;

import java.io.Serializable;

import com.capgemini.csd.hackaton.beans.Timestamp;
import com.capgemini.csd.hackaton.v3.Messages;

public class Message implements Serializable {

	private static final long serialVersionUID = -7027323607067414084L;

	private long timestamp;

	private int idTs;

	private int sensorType;

	private long value;

	private long interval;

	public Message(long timestamp, int sensorType, long value, int idTs) {
		super();
		this.timestamp = timestamp;
		this.interval = timestamp / Messages.MS_PAR_LOT;
		this.sensorType = sensorType;
		this.value = value;
		this.idTs = idTs;
	}

	public Message() {
		super();
	}

	public long getInterval() {
		return interval;
	}

	public Timestamp getTs() {
		return new Timestamp(timestamp, idTs);
	}

	public long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
		this.interval = timestamp / Messages.MS_PAR_LOT;
	}

	public int getIdTs() {
		return idTs;
	}

	public void setIdTs(int idTs) {
		this.idTs = idTs;
	}

	public int getSensorType() {
		return sensorType;
	}

	public void setSensorType(int sensorType) {
		this.sensorType = sensorType;
	}

	public long getValue() {
		return value;
	}

	public void setValue(long value) {
		this.value = value;
	}

	@Override
	public String toString() {
		return "Message [timestamp=" + timestamp + ", idTs=" + idTs + ", sensorType=" + sensorType + ", value=" + value
				+ "]";
	}

}