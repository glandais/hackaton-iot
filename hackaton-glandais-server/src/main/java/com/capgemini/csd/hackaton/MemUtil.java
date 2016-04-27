package com.capgemini.csd.hackaton;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.stream.Stream;

import com.capgemini.csd.hackaton.v2.Summary;

public class MemUtil {

	// id incrémental, si deux messages avec le même timestamp
	private static final AtomicLong currentId = new AtomicLong();

	public static final Map<Integer, Summary> getSummary(NavigableMap<UUID, UUID> memoryMap) {
		long time = System.currentTimeMillis();
		long lo = time - 3600 * 1000;
		long hi = time;
		UUID from = new UUID(lo, 0);
		UUID to = new UUID(hi, 0);

		Stream<UUID> stream = memoryMap.subMap(from, to).values().stream();

		Map<Long, List<Long>> messagesPerSensor = stream
				.collect(groupingBy(UUID::getMostSignificantBits, mapping(UUID::getLeastSignificantBits, toList())));
		Stream<Entry<Long, List<Long>>> grouped = messagesPerSensor.entrySet().stream();

		return grouped.collect(toMap(e -> e.getKey().intValue(), e -> e.getValue().stream()
				.collect(() -> new Summary(e.getKey().intValue()), Summary::accept, Summary::combine)));
	}

	public static final UUID add(Map<UUID, UUID> map, Map<String, Object> message, Lock w) {
		long timestamp = ((Date) message.get("timestamp")).getTime();
		long sensorId = ((Number) message.get("sensorType")).longValue();
		long value = ((Number) message.get("value")).longValue();

		UUID timeUUID = new UUID(timestamp, currentId.getAndIncrement());
		UUID valueUUID = new UUID(sensorId, value);

		if (w != null) {
			w.lock();
		}
		try {
			map.put(timeUUID, valueUUID);
		} finally {
			if (w != null) {
				w.unlock();
			}
		}

		return timeUUID;
	}
}
