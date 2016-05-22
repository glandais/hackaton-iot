package com.capgemini.csd.hackaton.v3.messages;

import java.io.Serializable;

import com.capgemini.csd.hackaton.beans.Timestamp;

public class Message implements Serializable {

	private static final long serialVersionUID = -7027323607067414084L;

	private String id;

	private long timestamp;

	private int idTs;

	private int sensorType;

	private long value;

	public Message(String id, long timestamp, int sensorType, long value, int idTs) {
		super();
		this.id = id;
		this.timestamp = timestamp;
		this.sensorType = sensorType;
		this.value = value;
		this.idTs = idTs;
	}

	public Message() {
		super();
	}

	public Timestamp getTs() {
		return new Timestamp(timestamp, idTs);
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
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
		return "Message [id=" + id + ", timestamp=" + timestamp + ", idTs=" + idTs + ", sensorType=" + sensorType
				+ ", value=" + value + "]";
	}

}