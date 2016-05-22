package com.capgemini.csd.hackaton.v3.messages.mapdb;

import java.io.File;
import java.util.Map;
import java.util.NavigableMap;

import org.mapdb.BTreeMap;
import org.mapdb.DB;
import org.mapdb.DBMaker;

import com.capgemini.csd.hackaton.beans.Timestamp;
import com.capgemini.csd.hackaton.beans.Value;
import com.capgemini.csd.hackaton.beans.mapdb.SerializerTimestamp;
import com.capgemini.csd.hackaton.beans.mapdb.SerializerValue;
import com.capgemini.csd.hackaton.v3.Messages;
import com.capgemini.csd.hackaton.v3.messages.AbstractMessages;

public class MessagesMapDB extends AbstractMessages implements Messages {

	private BTreeMap<Timestamp, Value> map;
	private DB db;

	public MessagesMapDB(File file) {
		db = DBMaker.fileDB(file).fileMmapEnable().concurrencyDisable().make();
		map = db.treeMap("messages", new SerializerTimestamp(), new SerializerValue()).createOrOpen();
	}

	@Override
	protected NavigableMap<Timestamp, Value> getMap() {
		return map;
	}

	public void close() {
		db.close();
	}

	public void putAll(Map<Timestamp, Value> map2) {
		map.putAll(map2);
	}

}
