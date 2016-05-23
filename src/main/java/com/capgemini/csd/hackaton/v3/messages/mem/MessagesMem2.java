package com.capgemini.csd.hackaton.v3.messages.mem;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.capgemini.csd.hackaton.beans.Timestamp;
import com.capgemini.csd.hackaton.beans.Value;
import com.capgemini.csd.hackaton.v3.Messages;
import com.capgemini.csd.hackaton.v3.messages.Message;
import com.capgemini.csd.hackaton.v3.summaries.Summaries;
import com.google.common.collect.Iterables;

import net.openhft.koloboke.collect.map.hash.HashIntObjMaps;

public class MessagesMem2 implements Messages {

	private Map<Integer, SensorMem> map = HashIntObjMaps.newMutableMap();

	@Override
	public void add(Message message) {
		int sensorType = message.getSensorType();
		SensorMem sensorMem = map.get(sensorType);
		if (sensorMem == null) {
			sensorMem = new SensorMem();
			map.put(sensorType, sensorMem);
		}
		sensorMem.add(message);
	}

	@Override
	public Summaries getSummaries(long from, long to) {
		Summaries summaries = new Summaries();
		for (SensorMem entry : map.values()) {
			summaries.combine(entry.getSummaries(from, to));
		}
		return summaries;
	}

	public Iterable<Map.Entry<Timestamp, Value>> getValues() {
		List<Iterable<Map.Entry<Timestamp, Value>>> inputs = new ArrayList<>();
		for (SensorMem sensorMem : map.values()) {
			inputs.add(sensorMem.getMap().entrySet());
		}
		return Iterables.concat(inputs);
	}
}
