package com.capgemini.csd.hackaton.v2.store;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.capgemini.csd.hackaton.client.Summary;
import com.capgemini.csd.hackaton.v2.message.Message;

public class StoreNoop implements Store {

	@Override
	public Map<Integer, Summary> getSummary(long timestamp, Integer duration) {
		return Collections.emptyMap();
	}

	@Override
	public boolean containsId(String id) {
		return false;
	}

	@Override
	public void indexMessages(List<Message> messages) {
	}

}
