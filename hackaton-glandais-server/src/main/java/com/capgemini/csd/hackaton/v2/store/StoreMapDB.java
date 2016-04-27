package com.capgemini.csd.hackaton.v2.store;

import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.mapdb.BTreeMap;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;

import com.capgemini.csd.hackaton.MemUtil;
import com.capgemini.csd.hackaton.v2.Summary;

public class StoreMapDB implements Store {

	private HTreeMap.KeySet<String> ids;

	private BTreeMap<UUID, UUID> map;

	public void init(String dossier) {
		DB db = DBMaker.fileDB(dossier + "/messages").make();
		ids = db.hashSet("ids", Serializer.STRING).createOrOpen();
		map = db.treeMap("messages").keySerializer(Serializer.UUID).valueSerializer(Serializer.UUID).createOrOpen();
	}

	@Override
	public Map<Integer, Summary> getSummary() {
		return MemUtil.getSummary(map);
	}

	@Override
	public boolean containsId(String id) {
		return ids.contains(id);
	}

	@Override
	public void indexMessages(List<Map<String, Object>> messages) {
		Map<UUID, UUID> newItems = new IdentityHashMap<>(messages.size());
		messages.parallelStream().forEach(message -> {
			String id = (String) message.get("id");
			ids.add(id);
			MemUtil.add(newItems, message, null);
		});
		map.putAll(newItems);
	}

}
