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
@NamedQueries({ @NamedQuery(name = "Message.exists", query = "SELECT COUNT(m.id) FROM Message m WHERE m.id = :id"),
		@NamedQuery(name = "Message.summary", query = "SELECT m.sensorType, COUNT(m.id), SUM(m.value), MIN(m.value), MAX(m.value) FROM Message m "
				+ "WHERE m.ts BETWEEN :start AND :end GROUP BY m.sensorType") })
public class Message {

	@Id
	@GeneratedValue
	private long realId;

	@Basic
	@Index(unique = "true")
	private String id;

	@Basic
	@Index
	private Date ts;

	@Basic
	@Index
	private int sensorType;

	@Basic
	private long value;

	public Message(String id, Date ts, int sensorType, long value) {
		super();
		this.id = id;
		this.ts = ts;
		this.sensorType = sensorType;
		this.value = value;
	}

	public Message() {
		super();
	}

}
