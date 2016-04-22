package com.capgemini.csd.hackaton.v2.store;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.capgemini.csd.hackaton.v2.Summary;

public class ChainedStore implements Store {

	private Store mainStore;

	private Store[] stores;

	private ExecutorService executor;

	public ChainedStore(Store mainStore, Store... stores) {
		super();
		this.mainStore = mainStore;
		this.stores = stores;
		int threads = Runtime.getRuntime().availableProcessors();
		executor = Executors.newFixedThreadPool(threads);
	}

	@Override
	public Map<Integer, Summary> getSummary() {
		return mainStore.getSummary();
	}

	@Override
	public boolean containsId(String id) {
		return mainStore.containsId(id);
	}

	@Override
	public void indexMessages(List<Map<String, Object>> messages) {
		mainStore.indexMessages(messages);
		for (Store store : stores) {
			executor.submit(() -> store.indexMessages(messages));
		}
	}

}
