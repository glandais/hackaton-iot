package com.capgemini.csd.hackaton.v2.store;

import java.util.Date;

import javax.jdo.annotations.Index;
import javax.persistence.Basic;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.NamedQueries;
import javax.persistence.NamedQuery;

@Entity
@NamedQueries({ @NamedQuery(name = "MessageHash.exists", query = "SELECT m.id FROM MessageHash m WHERE m.hash = :hash"),
		@NamedQuery(name = "MessageHash.summary", query = "SELECT m.sensorType, COUNT(m.id), SUM(m.value), MIN(m.value), MAX(m.value) FROM MessageHash m "
				+ "WHERE m.timestamp BETWEEN :start AND :end GROUP BY m.sensorType") })
public class MessageHash {

	@Id
	@GeneratedValue
	private Long realId;

	@Basic
	@Index
	private Integer hash;

	@Basic
	private String id;

	@Basic
	@Index
	private Date timestamp;

	@Basic
	@Index
	private int sensorType;

	@Basic
	private long value;

	public MessageHash(String id, Date timestamp, int sensorType, long value) {
		super();
		this.id = id;
		this.hash = id.hashCode();
		this.timestamp = timestamp;
		this.sensorType = sensorType;
		this.value = value;
	}

	public MessageHash() {
		super();
	}

	public String getId() {
		return id;
	}

}
