package com.capgemini.csd.hackaton.v3.messages.mem;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.capgemini.csd.hackaton.beans.Timestamp;
import com.capgemini.csd.hackaton.beans.Value;
import com.capgemini.csd.hackaton.v3.Messages;
import com.capgemini.csd.hackaton.v3.messages.Message;
import com.capgemini.csd.hackaton.v3.summaries.Summaries;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Iterables;

public class MessagesMem2 implements Messages {

	protected static final class SensorMemLoader extends CacheLoader<Integer, SensorMem> {
		@Override
		public SensorMem load(Integer key) throws Exception {
			return new SensorMem();
		}
	}

	protected static final SensorMemLoader SENSOR_MEM_LOADER = new SensorMemLoader();

	private LoadingCache<Integer, SensorMem> map = CacheBuilder.newBuilder().build(SENSOR_MEM_LOADER);

	@Override
	public void add(Message message) {
		SensorMem sensorMem = map.getUnchecked(message.getSensorType());
		sensorMem.add(message);
	}

	@Override
	public Summaries getSummaries(long from, long to) {
		Summaries summaries = new Summaries();
		for (SensorMem entry : map.asMap().values()) {
			summaries.combine(entry.getSummaries(from, to));
		}
		return summaries;
	}

	public Iterable<Map.Entry<Timestamp, Value>> getValues() {
		List<Iterable<Map.Entry<Timestamp, Value>>> inputs = new ArrayList<>();
		for (SensorMem sensorMem : map.asMap().values()) {
			inputs.add(sensorMem.getMap().entrySet());
		}
		return Iterables.concat(inputs);
	}
}
