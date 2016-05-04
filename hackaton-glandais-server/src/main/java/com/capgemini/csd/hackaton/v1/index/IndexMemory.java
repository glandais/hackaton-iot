package com.capgemini.csd.hackaton.v1.index;

import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.LongSummaryStatistics;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.boon.core.Sys;
import org.boon.json.JsonFactory;

import com.capgemini.csd.hackaton.Util;
import com.google.common.collect.ImmutableMap;

import net.openhft.koloboke.collect.set.hash.HashObjSets;

public class IndexMemory implements Index {

	private ReentrantLock idLock = new ReentrantLock();
	private NavigableMap<UUID, UUID> map;
	private Set<String> ids;
	private static AtomicLong currentId = new AtomicLong();

	public IndexMemory() {
		super();
	}

	@Override
	public void init(String dossier) {
		map = Collections.synchronizedNavigableMap(new TreeMap<>());
		ids = Collections.synchronizedSet(HashObjSets.newMutableSet());
		Thread thread = new Thread(() -> {
			// boucle de nettoyage de la map
			while (true) {
				boolean invalidFirst = isInvalidFirst();
				if (invalidFirst) {
					while (invalidFirst) {
						synchronized (map) {
							if (isInvalidFirst()) {
								map.pollFirstEntry();
							}
						}
						invalidFirst = isInvalidFirst();
					}
				} else {
					Sys.sleep(10000L);
				}
			}
		});
		thread.setName("indexer");
		thread.setPriority(Thread.MIN_PRIORITY);
		thread.start();
	}

	private boolean isInvalidFirst() {
		if (!map.isEmpty()) {
			Entry<UUID, UUID> firstEntry = map.firstEntry();
			if (firstEntry != null) {
				long lo = System.currentTimeMillis() - 3600 * 1000;
				return firstEntry.getKey().getLeastSignificantBits() < lo;
			}
		}
		return false;
	}

	@Override
	public boolean isInMemory() {
		return true;
	}

	@Override
	public void index(String json) {
		Map<?, ?> message = Util.fromJson(json);
		String id = (String) message.get("id");
		idLock.lock();
		try {
			if (ids.contains(id)) {
				throw new RuntimeException("ID existant : " + id);
			}
			ids.add(id);
		} finally {
			idLock.unlock();
		}
		long time = System.currentTimeMillis();
		long lo = time - 3600 * 1000;
		long timestamp = ((Date) message.get("timestamp")).getTime();
		if (timestamp > lo) {
			long sensorId = ((Number) message.get("sensorType")).longValue();
			long value = ((Number) message.get("value")).longValue();
			synchronized (map) {
				map.put(new UUID(timestamp, currentId.incrementAndGet()), new UUID(sensorId, value));
			}
		}
	}

	@Override
	public String getSynthese() {
		long time = System.currentTimeMillis();
		long lo = time - 3600 * 1000;
		long hi = time;
		UUID from = new UUID(lo, 0);
		UUID to = new UUID(hi, 0);
		Stream<UUID> stream = map.subMap(from, to).values().stream();

		Map<Long, LongSummaryStatistics> statsMap;
		synchronized (map) {
			statsMap = stream.collect(Collectors.groupingBy(e -> e.getMostSignificantBits())).entrySet().stream()
					.collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().stream()
							.collect(Collectors.summarizingLong(UUID::getLeastSignificantBits))));
		}

		List<Map<String, Object>> syntheses = statsMap.entrySet().stream()
				.map(e -> new ImmutableMap.Builder<String, Object>().put("sensorType", e.getKey())
						.put("minValue", e.getValue().getMin()).put("maxValue", e.getValue().getMax())
						.put("mediumValue", Math.round(e.getValue().getAverage())).build())
				.collect(Collectors.toList());

		return JsonFactory.toJson(syntheses);
	}

	@Override
	public void close() {
	}

	@Override
	public long getSize() {
		return map.size();
	}
}
