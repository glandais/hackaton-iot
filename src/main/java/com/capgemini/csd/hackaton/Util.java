package com.capgemini.csd.hackaton;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.boon.json.JsonFactory;
import org.joda.time.DateTime;

import com.capgemini.csd.hackaton.client.AbstractClient;
import com.capgemini.csd.hackaton.client.Summary;
import com.capgemini.csd.hackaton.v2.message.Message;
import com.capgemini.csd.hackaton.v2.message.Timestamp;
import com.capgemini.csd.hackaton.v2.message.Value;
import com.squareup.moshi.JsonReader;

import okio.Buffer;

public class Util {

	// id incrémental, si deux messages avec le même timestamp
	private static final AtomicInteger currentId = new AtomicInteger();

	public static void main(String[] args) {
		String json = AbstractClient.getMessage(true);
		System.out.println(json);
		System.out.println(messageFromJson(json).toString());
		//		TreeMap<Timestamp, Value> map = new TreeMap<>();
		//		map.put(new Timestamp(1), new Value(0, Long.MAX_VALUE - 200));
		//		map.put(new Timestamp(1), new Value(0, Long.MAX_VALUE - 100));
		//		Map<Integer, Summary> summary = getSummary(map, 0, 1000);
		//		System.out.println(summary);
	}

	public static final Map<Integer, Summary> getSummary(NavigableMap<Timestamp, Value> memoryMap, long timestamp,
			Integer duration) {
		long lo = timestamp;
		long hi = timestamp + duration * 1000;
		Timestamp from = new Timestamp(lo, 0);
		Timestamp to = new Timestamp(hi, Integer.MAX_VALUE);

		Stream<Value> stream = memoryMap.subMap(from, to).values().stream();

		Map<Integer, Summary> map = stream.collect(groupingBy(Value::getSensorId,
				mapping(Value::getValue, Collector.of(Summary::new, Summary::accept, Summary::combine2))));
		for (Entry<Integer, Summary> entry : map.entrySet()) {
			entry.getValue().setSensorType(entry.getKey());
		}
		return map;
	}

	public static final void add(Map<Timestamp, Value> map, Message message, Lock w) {
		Timestamp ts = new Timestamp(message.getTimestamp(), message.getIdTs());
		Value val = new Value(message.getSensorType(), message.getValue());

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

	public static enum State {
		INIT, READING_KEY, WAITING_COLON, WAITING_VALUE_START, READING_VALUE_STRING, READING_VALUE_STRING_ESCAPED, READING_VALUE_NUMBER, VALUE;
	}

	public static Message messageFromJson(String json) {
		JsonReader jsonReader = JsonReader.of(new Buffer().writeUtf8(json));
		String id = null;
		long timestamp = 0;
		int sensorType = 0;
		long value = 0;
		int idTs = currentId.getAndIncrement();
		try {
			jsonReader.beginObject();
			while (jsonReader.hasNext()) {
				String name = jsonReader.nextName();
				if (name.equals("id")) {
					id = jsonReader.nextString();
				} else if (name.equals("timestamp")) {
					String tsFormatted = jsonReader.nextString();
					timestamp = DateTime.parse(tsFormatted).getMillis();
				} else if (name.equals("sensorType")) {
					sensorType = jsonReader.nextInt();
				} else if (name.equals("value")) {
					value = jsonReader.nextLong();
				}
			}
			jsonReader.close();
		} catch (IOException e) {
		}
		return new Message(id, timestamp, sensorType, value, idTs);
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
