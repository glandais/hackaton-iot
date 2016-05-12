package com.capgemini.csd.hackaton.v2.message;

import java.util.Collections;
import java.util.Map;

import javax.jdo.annotations.Index;
import javax.persistence.Basic;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.NamedQueries;
import javax.persistence.NamedQuery;
import javax.persistence.Transient;

@Entity
@NamedQueries({ @NamedQuery(name = "Message.exists", query = "SELECT COUNT(m.id) FROM Message m WHERE m.id = :id"),
		@NamedQuery(name = "Message.summary", query = "SELECT m.sensorType, COUNT(m.id), SUM(m.value), MIN(m.value), MAX(m.value) FROM Message m "
				+ "WHERE m.timestamp BETWEEN :start AND :end GROUP BY m.sensorType") })
public class Message {

	private static final long serialVersionUID = -7027323607067414084L;

	@Id
	@GeneratedValue
	private Long realId;

	@Basic
	@Index(unique = "true")
	private String id;

	@Basic
	@Index
	private long timestamp;

	@Transient
	private int idTs;

	@Basic
	@Index
	private int sensorType;

	@Basic
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

	public Long getRealId() {
		return realId;
	}

	public void setRealId(Long realId) {
		this.realId = realId;
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

	public Map<String, Object> getMap() {
		// FIXME
		return Collections.emptyMap();
	}

}