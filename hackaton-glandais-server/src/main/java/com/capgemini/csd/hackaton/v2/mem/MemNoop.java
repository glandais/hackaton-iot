package com.capgemini.csd.hackaton.v2.mem;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.capgemini.csd.hackaton.client.Summary;
import com.capgemini.csd.hackaton.v2.message.Message;
import com.capgemini.csd.hackaton.v2.message.Timestamp;

public class MemNoop implements Mem {
	@Override
	public Map<Integer, Summary> getSummary(long timestamp, Integer duration) {
		return Collections.emptyMap();
	}

	@Override
	public boolean containsId(String id) {
		return false;
	}

	@Override
	public void removeMessages(List<Message> messages) {
	}

	@Override
	public void putId(String id) {
	}

	@Override
	public Timestamp index(Map<String, Object> message) {
		return null;
	}

	@Override
	public long getSize() {
		return 0;
	}

	@Override
	public void close() {
	}
}