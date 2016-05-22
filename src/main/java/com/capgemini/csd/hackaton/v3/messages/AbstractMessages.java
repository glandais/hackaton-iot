package com.capgemini.csd.hackaton.v3.messages;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;

import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collector;
import java.util.stream.Stream;

import com.capgemini.csd.hackaton.beans.Timestamp;
import com.capgemini.csd.hackaton.beans.Value;
import com.capgemini.csd.hackaton.v3.Messages;
import com.capgemini.csd.hackaton.v3.summaries.Summaries;
import com.capgemini.csd.hackaton.v3.summaries.Summary;

public abstract class AbstractMessages implements Messages {

	private final ReentrantLock lock = new ReentrantLock();

	protected void add(Message message, NavigableMap<Timestamp, Value> map) {
		Timestamp ts = new Timestamp(message.getTimestamp(), message.getIdTs());
		Value val = new Value(message.getSensorType(), message.getValue());

		lock.lock();
		try {
			map.put(ts, val);
		} finally {
			lock.unlock();
		}
	}

	protected Summaries getSummaries(long from, long to, NavigableMap<Timestamp, Value> map) {
		Timestamp fromTs = new Timestamp(from, 0);
		Timestamp toTs = new Timestamp(to, Integer.MAX_VALUE);

		lock.lock();
		Map<Integer, Summary> result;
		try {
			Stream<Value> stream = map.subMap(fromTs, toTs).values().stream();
			result = stream.collect(groupingBy(Value::getSensorId,
					mapping(Value::getValue, Collector.of(Summary::new, Summary::accept, Summary::combine2))));
		} finally {
			lock.unlock();
		}
		for (Entry<Integer, Summary> entry : result.entrySet()) {
			entry.getValue().setSensorType(entry.getKey());
		}
		return new Summaries(result);
	}

}
