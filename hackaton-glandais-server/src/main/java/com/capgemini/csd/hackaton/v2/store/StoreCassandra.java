package com.capgemini.csd.hackaton.v2.store;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.service.CassandraDaemon;

import com.capgemini.csd.hackaton.client.Summary;
import com.capgemini.csd.hackaton.v2.message.Message;

public class StoreCassandra implements Store {

	private CassandraDaemon cassandraDaemon;

	public void init(String dossier) {
		cassandraDaemon = new CassandraDaemon(true);
		cassandraDaemon.activate();
	}

	@Override
	public Map<Integer, Summary> getSummary(long timestamp, Integer duration) {
		// TODO Auto-generated method stub
		return Collections.emptyMap();
	}

	@Override
	public boolean containsId(String id) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void indexMessages(List<Message> messages) {
		// TODO Auto-generated method stub

	}

}
