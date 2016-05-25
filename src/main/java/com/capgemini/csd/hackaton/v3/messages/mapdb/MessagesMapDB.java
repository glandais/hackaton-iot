package com.capgemini.csd.hackaton.v3.messages.mapdb;

import java.io.File;

import org.mapdb.BTreeMap;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.DBMaker.Maker;

import com.capgemini.csd.hackaton.beans.Timestamp;
import com.capgemini.csd.hackaton.beans.Value;
import com.capgemini.csd.hackaton.beans.mapdb.SerializerTimestamp;
import com.capgemini.csd.hackaton.beans.mapdb.SerializerValue;
import com.capgemini.csd.hackaton.v3.Messages;
import com.capgemini.csd.hackaton.v3.messages.AbstractMessages;
import com.capgemini.csd.hackaton.v3.messages.Message;
import com.capgemini.csd.hackaton.v3.summaries.Summaries;

public class MessagesMapDB extends AbstractMessages implements Messages {

	private BTreeMap<Timestamp, Value> map;
	private DB db;

	public MessagesMapDB(File file, boolean transactional) {
		Maker config = DBMaker.fileDB(file).fileMmapEnable().concurrencyDisable().checksumHeaderBypass();
		if (transactional) {
			config.transactionEnable();
		}
		db = config.make();
		map = db.treeMap("messages", new SerializerTimestamp(), new SerializerValue()).createOrOpen();
	}

	@Override
	public void add(Message message) {
		add(message, map);
	}

	@Override
	public Summaries getSummaries(long from, long to) {
		return getSummaries(from, to, map);
	}

	public void close() {
		db.close();
	}

	public void put(Timestamp ts, Value value) {
		map.put(ts, value);
	}

}
