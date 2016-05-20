package com.capgemini.csd.hackaton.v2.store;

import java.util.List;
import java.util.Map;

import org.mapdb.BTreeMap;
import org.mapdb.DB;
import org.mapdb.DBMaker;

import com.capgemini.csd.hackaton.Util;
import com.capgemini.csd.hackaton.client.Summary;
import com.capgemini.csd.hackaton.v2.message.Message;
import com.capgemini.csd.hackaton.v2.message.Timestamp;
import com.capgemini.csd.hackaton.v2.message.Value;
import com.capgemini.csd.hackaton.v2.store.mapdb.SerializerTimestamp;
import com.capgemini.csd.hackaton.v2.store.mapdb.SerializerValue;

public class StoreMapDB implements Store {

	private BTreeMap<Timestamp, Value> map;
	private DB db;

	@Override
	public void init(String dossier) {
		db = DBMaker.fileDB(dossier + "/mapdb").fileMmapEnable().concurrencyDisable().make();
		map = db.treeMap("messages", new SerializerTimestamp(), new SerializerValue()).createOrOpen();
	}

	@Override
	public Map<Integer, Summary> getSummary(long timestamp, Integer duration) {
		return Util.getSummary(map, timestamp, duration);
	}

	@Override
	public boolean containsId(String id) {
		return false;
	}

	@Override
	public void indexMessages(List<Message> messages) {
		for (Message message : messages) {
			Util.add(map, message, null);
		}
	}

	@Override
	public void close() {
		db.close();
	}

}
