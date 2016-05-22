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
import com.capgemini.csd.hackaton.client.Summary;
import com.capgemini.csd.hackaton.v3.Messages;
import com.capgemini.csd.hackaton.v3.summaries.Summaries;

public abstract class AbstractMessages implements Messages {

	private final ReentrantLock lock = new ReentrantLock();

	protected abstract NavigableMap<Timestamp, Value> getMap();

	@Override
	public void add(Message message) {
		Timestamp ts = new Timestamp(message.getTimestamp(), message.getIdTs());
		Value val = new Value(message.getSensorType(), message.getValue());

		lock.lock();
		try {
			getMap().put(ts, val);
		} finally {
			lock.unlock();
		}
	}

	@Override
	public Summaries getSummaries(long from, long to) {
		Timestamp fromTs = new Timestamp(from, 0);
		Timestamp toTs = new Timestamp(to, Integer.MAX_VALUE);

		lock.lock();
		Map<Integer, Summary> map;
		try {
			Stream<Value> stream = getMap().subMap(fromTs, toTs).values().stream();
			map = stream.collect(groupingBy(Value::getSensorId,
					mapping(Value::getValue, Collector.of(Summary::new, Summary::accept, Summary::combine2))));
		} finally {
			lock.unlock();
		}
		for (Entry<Integer, Summary> entry : map.entrySet()) {
			entry.getValue().setSensorType(entry.getKey());
		}
		return new Summaries(map);
	}

}
