package com.capgemini.csd.hackaton;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.locks.Lock;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.boon.json.JsonFactory;
import org.joda.time.DateTime;

import com.capgemini.csd.hackaton.client.Summary;
import com.capgemini.csd.hackaton.v2.message.Timestamp;
import com.capgemini.csd.hackaton.v2.message.Value;

public class Util {

	public static void main(String[] args) {
		TreeMap<Timestamp, Value> map = new TreeMap<>();
		map.put(new Timestamp(1), new Value(0, Long.MAX_VALUE - 200));
		map.put(new Timestamp(1), new Value(0, Long.MAX_VALUE - 100));
		Map<Integer, Summary> summary = getSummary(map, 0, 1000);
		System.out.println(summary);
	}

	public static final Map<Integer, Summary> getSummary(NavigableMap<Timestamp, Value> memoryMap, long timestamp,
			Integer duration) {
		long lo = timestamp;
		long hi = timestamp + duration * 1000;
		Timestamp from = new Timestamp(lo, 0);
		Timestamp to = new Timestamp(hi, Long.MAX_VALUE);

		Stream<Value> stream = memoryMap.subMap(from, to).values().stream();

		Map<Integer, Summary> map = stream.collect(groupingBy(Value::getSensorId,
				mapping(Value::getValue, Collector.of(Summary::new, Summary::accept, Summary::combine2))));
		for (Entry<Integer, Summary> entry : map.entrySet()) {
			entry.getValue().setSensorType(entry.getKey());
		}
		return map;
	}

	public static final Timestamp add(Map<Timestamp, Value> map, Map<String, Object> message, Lock w) {
		long timestamp = (long) message.get("timestamp");
		int sensorId = ((Number) message.get("sensorType")).intValue();
		long value = ((Number) message.get("value")).longValue();

		Timestamp ts = new Timestamp(timestamp);
		Value val = new Value(sensorId, value);

		if (w != null) {
			w.lock();
		}
		try {
			map.put(ts, val);
		} finally {
			if (w != null) {
				w.unlock();
			}
		}

		return ts;
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
			map.put("timestamp", DateTime.parse((String) map.get("timestamp")).getMillis());
		}
		if (map.get("timestamp") instanceof Date) {
			map.put("timestamp", ((Date) map.get("timestamp")).getTime());
		}
		return map;
	}

}
