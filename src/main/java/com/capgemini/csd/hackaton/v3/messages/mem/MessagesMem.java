package com.capgemini.csd.hackaton.v3.messages.mem;

import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.TreeMap;

import com.capgemini.csd.hackaton.beans.Timestamp;
import com.capgemini.csd.hackaton.beans.Value;
import com.capgemini.csd.hackaton.v3.Messages;
import com.capgemini.csd.hackaton.v3.messages.AbstractMessages;
import com.capgemini.csd.hackaton.v3.messages.Message;
import com.capgemini.csd.hackaton.v3.summaries.Summaries;

public class MessagesMem extends AbstractMessages implements Messages {

	private NavigableMap<Timestamp, Value> map = new TreeMap<>();

	@Override
	public void add(Message message) {
		add(message, map);
	}

	@Override
	public Summaries getSummaries(long from, long to) {
		return getSummaries(from, to, map);
	}

	public Iterable<Entry<Timestamp, Value>> getValues() {
		return map.entrySet();
	}

}
