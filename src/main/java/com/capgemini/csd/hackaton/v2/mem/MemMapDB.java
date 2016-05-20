package com.capgemini.csd.hackaton.v2.mem;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.mapdb.BTreeMap;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.capgemini.csd.hackaton.Util;
import com.capgemini.csd.hackaton.client.Summary;
import com.capgemini.csd.hackaton.v2.AbstractIOTServer;
import com.capgemini.csd.hackaton.v2.message.Message;
import com.capgemini.csd.hackaton.v2.message.Timestamp;
import com.capgemini.csd.hackaton.v2.message.Value;
import com.capgemini.csd.hackaton.v2.store.mapdb.SerializerTimestamp;
import com.capgemini.csd.hackaton.v2.store.mapdb.SerializerValue;

import net.openhft.koloboke.collect.set.hash.HashObjSets;

public class MemMapDB implements Mem {

	public final static Logger LOGGER = LoggerFactory.getLogger(MemMapDB.class);

	private final ReentrantReadWriteLock rwl = new ReentrantReadWriteLock();
	private final Lock readLock = rwl.readLock();
	private final Lock writeLock = rwl.writeLock();

	private BTreeMap<Timestamp, Value> memoryMap;
	private DB db;

	// correspondance id du message/UUID de la memoryMap
	private Set<String> memoryIds;

	public MemMapDB() {
		db = DBMaker.heapDB().concurrencyDisable().make();
		memoryMap = db.treeMap("messages", new SerializerTimestamp(), new SerializerValue()).createOrOpen();
		if (AbstractIOTServer.TEST_ID) {
			memoryIds = Collections.synchronizedSet(HashObjSets.newMutableSet());
		}
	}

	protected <T> T read(Supplier<T> read) {
		readLock.lock();
		try {
			return read.get();
		} finally {
			readLock.unlock();
		}
	}

	protected <T> T write(Supplier<T> write) {
		writeLock.lock();
		try {
			return write.get();
		} finally {
			writeLock.unlock();
		}
	}

	public boolean containsId(String id) {
		if (AbstractIOTServer.TEST_ID) {
			return memoryIds.contains(id);
		} else {
			return false;
		}
	}

	public void putId(String id) {
		if (AbstractIOTServer.TEST_ID) {
			memoryIds.add(id);
		}
	}

	public void index(Message message) {
		Util.add(memoryMap, message, writeLock);
	}

	public void removeMessages(List<Message> messages) {
		List<Timestamp> uuids = messages.stream().map(m -> m.getTs()).filter(o -> (o != null))
				.collect(Collectors.toList());
		write(() -> memoryMap.keySet().removeAll(uuids));

		if (AbstractIOTServer.TEST_ID) {
			List<String> ids = messages.stream().map(m -> m.getId()).filter(o -> (o != null))
					.collect(Collectors.toList());
			write(() -> memoryIds.removeAll(ids));
		}
	}

	public Map<Integer, Summary> getSummary(long timestamp, Integer duration) {
		return read(() -> Util.getSummary(memoryMap, timestamp, duration));
	}

	@Override
	public void close() {
		db.close();
		if (AbstractIOTServer.TEST_ID) {
			memoryIds.clear();
		}
	}
}
