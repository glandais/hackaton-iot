package com.capgemini.csd.hackaton.v3.messages.mem;

import java.util.NavigableMap;
import java.util.TreeMap;

import com.capgemini.csd.hackaton.beans.Timestamp;
import com.capgemini.csd.hackaton.beans.Value;
import com.capgemini.csd.hackaton.v3.Messages;
import com.capgemini.csd.hackaton.v3.messages.AbstractMessages;

public class MessagesMem extends AbstractMessages implements Messages {

	private NavigableMap<Timestamp, Value> map = new TreeMap<>();

	@Override
	public NavigableMap<Timestamp, Value> getMap() {
		return map;
	}

}
