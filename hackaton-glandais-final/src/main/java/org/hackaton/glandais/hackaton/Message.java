package org.hackaton.glandais.hackaton;

import java.util.Date;
import java.util.UUID;

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

	@Id
	@GeneratedValue
	private Long realId;

	@Basic
	@Index(unique = "true")
	private String id;

	@Basic
	@Index
	private Date timestamp;

	@Basic
	@Index
	private int sensorType;

	@Transient
	private UUID uuid;

	@Basic
	private long value;

	public Message(String id, Date timestamp, int sensorType, long value, UUID uuid) {
		super();
		this.id = id;
		this.timestamp = timestamp;
		this.sensorType = sensorType;
		this.value = value;
		this.uuid = uuid;
	}

	public Message() {
		super();
	}

	public Long getRealId() {
		return realId;
	}

	public String getId() {
		return id;
	}

	public Date getTimestamp() {
		return timestamp;
	}

	public int getSensorType() {
		return sensorType;
	}

	public UUID getUuid() {
		return uuid;
	}

	public long getValue() {
		return value;
	}

}
