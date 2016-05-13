package com.capgemini.csd.hackaton.v2.mem;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.capgemini.csd.hackaton.client.Summary;
import com.capgemini.csd.hackaton.v2.message.Message;

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
	public void index(Message message) {
	}

	@Override
	public long getSize() {
		return 0;
	}

	@Override
	public void close() {
	}
}