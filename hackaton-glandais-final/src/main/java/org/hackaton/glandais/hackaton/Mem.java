package org.hackaton.glandais.hackaton;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.openhft.koloboke.collect.set.hash.HashObjSets;

public class Mem {

	public final static Logger LOGGER = LoggerFactory.getLogger(Mem.class);

	private final ReentrantReadWriteLock rwl = new ReentrantReadWriteLock();
	private final Lock readLock = rwl.readLock();
	private final Lock writeLock = rwl.writeLock();

	// messages reçus non stockés
	private NavigableMap<UUID, UUID> memoryMap;

	// correspondance id du message/UUID de la memoryMap
	private Set<String> memoryIds;

	public Mem() {
		super();
		memoryMap = new TreeMap<>();
		memoryIds = Collections.synchronizedSet(HashObjSets.newMutableSet());
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
		return memoryIds.contains(id);
	}

	public void putId(String id) {
		memoryIds.add(id);
	}

	public UUID index(Map<String, Object> message) {
		return Util.add(memoryMap, message, writeLock);
	}

	public void removeMessages(List<Message> messages) {
		List<Object> uuids = messages.stream().map(m -> m.getUuid()).filter(o -> (o != null))
				.collect(Collectors.toList());
		write(() -> memoryMap.keySet().removeAll(uuids));
	}

	public Map<Integer, Summary> getSummary(long timestamp, Integer duration) {
		return read(() -> Util.getSummary(memoryMap, timestamp, duration));
	}

	public long getSize() {
		return read(() -> memoryMap.size());
	}

}
