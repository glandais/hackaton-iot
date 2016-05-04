package com.capgemini.csd.hackaton;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.boon.json.JsonFactory;
import org.joda.time.DateTime;

import com.capgemini.csd.hackaton.v2.synthese.Summary;

public class Util {

	// id incrémental, si deux messages avec le même timestamp
	private static final AtomicLong currentId = new AtomicLong();

	public static final Map<Integer, Summary> getSummary(NavigableMap<UUID, UUID> memoryMap, long timestamp,
			Integer duration) {
		long lo = timestamp;
		long hi = timestamp + duration * 1000;
		UUID from = new UUID(lo, 0);
		UUID to = new UUID(hi, Long.MAX_VALUE);

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

	public static Map<String, List<String>> parse(final String query) {
		return Arrays.asList(query.split("&")).stream().map(p -> p.split("=")).collect(
				Collectors.toMap(s -> decode(index(s, 0)), s -> Arrays.asList(decode(index(s, 1))), Util::mergeLists));
	}

	private static <T> List<T> mergeLists(final List<T> l1, final List<T> l2) {
		List<T> list = new ArrayList<>();
		list.addAll(l1);
		list.addAll(l2);
		return list;
	}

	private static <T> T index(final T[] array, final int index) {
		return index >= array.length ? null : array[index];
	}

	private static String decode(final String encoded) {
		try {
			return encoded == null ? null : URLDecoder.decode(encoded, "UTF-8");
		} catch (final UnsupportedEncodingException e) {
			throw new RuntimeException("Impossible: UTF-8 is a required encoding", e);
		}
	}

	public static Map fromJson(String message) {
		Map map = JsonFactory.fromJson(message, Map.class);
		if (map.get("timestamp") instanceof String) {
			map.put("timestamp", DateTime.parse((String) map.get("timestamp")).toDate());
		}
		return map;
	}

}
