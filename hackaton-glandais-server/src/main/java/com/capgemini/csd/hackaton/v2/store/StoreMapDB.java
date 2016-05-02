package com.capgemini.csd.hackaton.v2.store;

import java.io.File;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.mapdb.BTreeMap;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;

import com.capgemini.csd.hackaton.Util;
import com.capgemini.csd.hackaton.v2.synthese.Summary;

public class StoreMapDB implements Store {

	private final ReentrantReadWriteLock rwl = new ReentrantReadWriteLock();
	private final Lock r = rwl.readLock();
	private final Lock writeLock = rwl.writeLock();

	private HTreeMap.KeySet<String> ids;

	private BTreeMap<UUID, UUID> map;

	public void init(String dossier) {
		if (!new File(dossier).exists()) {
			new File(dossier).mkdirs();
		}
		DB db = DBMaker.fileDB(dossier + "/messages").make();
		ids = db.hashSet("ids", Serializer.STRING).createOrOpen();
		map = db.treeMap("messages").keySerializer(Serializer.UUID).valueSerializer(Serializer.UUID).createOrOpen();
	}

	@Override
	public Map<Integer, Summary> getSummary(long timestamp, Integer duration) {
		return Util.getSummary(map, timestamp, duration);
	}

	@Override
	public boolean containsId(String id) {
		r.lock();
		try {
			return ids.contains(id);
		} finally {
			r.unlock();
		}
	}

	@Override
	public void indexMessages(List<Map<String, Object>> messages) {
		Map<UUID, UUID> newItems = new IdentityHashMap<>(messages.size());
		messages.parallelStream().forEach(message -> {
			String id = (String) message.get("id");
			writeLock.lock();
			try {
				ids.add(id);
			} finally {
				writeLock.unlock();
			}
			Util.add(newItems, message, null);
		});
		map.putAll(newItems);
	}

}
