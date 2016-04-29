package com.capgemini.csd.hackaton.v2.mem;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;

import org.boon.core.Sys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.capgemini.csd.hackaton.MemUtil;
import com.capgemini.csd.hackaton.v2.synthese.Summary;

import net.openhft.koloboke.collect.map.hash.HashObjObjMaps;

public class MemBasic implements Mem {

	public final static Logger LOGGER = LoggerFactory.getLogger(MemBasic.class);

	private final ReentrantReadWriteLock rwl = new ReentrantReadWriteLock();
	private final Lock r = rwl.readLock();
	private final Lock writeLock = rwl.writeLock();

	// messages reçus non stockés par ES
	private NavigableMap<UUID, UUID> memoryMap;

	// correspondance id du message/UUID de la memoryMap
	private Map<Object, Object> memoryIds;

	public MemBasic(boolean autoClean) {
		super();
		memoryMap = new TreeMap<>();
		memoryIds = Collections.synchronizedMap(HashObjObjMaps.newMutableMap());
		startDaemon(autoClean);
	}

	protected void startDaemon(boolean autoClean) {
		Thread thread = new Thread(() -> {
			int previous = -1;
			// boucle de nettoyage de la map
			while (true) {
				int size = read(() -> memoryMap.size());
				if (size != previous) {
					LOGGER.info("Messages : " + size);
					previous = size;
				}
				if (autoClean) {
					boolean invalidFirst = isInvalidFirst();
					if (invalidFirst) {
						clean();
					} else {
						Sys.sleep(1000L);
					}
				} else {
					Sys.sleep(100L);
				}
			}
		});
		thread.setName("mem-cleaner");
		thread.setPriority(Thread.MIN_PRIORITY);
		thread.start();
	}

	protected <T> T read(Supplier<T> read) {
		r.lock();
		try {
			return read.get();
		} finally {
			r.unlock();
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

	public void clean() {
		boolean invalidFirst = isInvalidFirst();
		while (invalidFirst) {
			if (isInvalidFirst()) {
				write(() -> memoryMap.pollFirstEntry());
			}
			invalidFirst = isInvalidFirst();
		}
	}

	private boolean isInvalidFirst() {
		Entry<UUID, UUID> firstEntry = read(() -> {
			if (!memoryMap.isEmpty()) {
				return memoryMap.firstEntry();
			}
			return null;
		});
		if (firstEntry != null) {
			long lo = System.currentTimeMillis() - 3600 * 1000;
			return firstEntry.getKey().getMostSignificantBits() < lo;
		}
		return false;
	}

	@Override
	public boolean containsId(String id) {
		return memoryIds.containsKey(id);
	}

	@Override
	public void putId(String id) {
		memoryIds.put(id, null);
	}

	@Override
	public void index(Map<String, Object> message) {
		String id = (String) message.get("id");
		UUID uuid = MemUtil.add(memoryMap, message, writeLock);
		memoryIds.replace(id, uuid);
	}

	@Override
	public void removeMessages(List<Map<String, Object>> messages) {
		for (int i = 0; i < messages.size(); i++) {
			Object uuid = memoryIds.remove(messages.get(i).get("id"));
			if (uuid != null) {
				write(() -> memoryMap.remove(uuid));
			}
		}
	}

	@Override
	public Map<Integer, Summary> getSummary() {
		return read(() -> MemUtil.getSummary(memoryMap));
	}

	@Override
	public long getSize() {
		return read(() -> memoryMap.size());
	}

}
