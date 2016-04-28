package com.capgemini.csd.hackaton.v2.bench;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.boon.json.JsonFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.capgemini.csd.hackaton.client.AbstractClient;
import com.capgemini.csd.hackaton.v2.store.Store;
import com.capgemini.csd.hackaton.v2.store.StoreElasticSearch;
import com.capgemini.csd.hackaton.v2.store.StoreH2;
import com.capgemini.csd.hackaton.v2.store.StoreMapDB;
import com.google.common.base.Stopwatch;

public class StoreBench {

	public final static Logger LOGGER = LoggerFactory.getLogger(StoreBench.class);

	public final static Random R = new Random();

	public static void main(String[] args) {
		bench(getStoreES());
		bench(getStoreH2());
		bench(getStoreMapDB());
	}

	private static void bench(Store store) {
		Stopwatch stopwatch = Stopwatch.createUnstarted();
		List<String> ids = new ArrayList<>();
		for (int i = -20; i < 100; i++) {
			if (i == 0) {
				stopwatch = Stopwatch.createUnstarted();
			}
			List<Map<String, Object>> messages = new ArrayList<>(1001);
			for (int j = 0; j < 1000; j++) {
				String message = AbstractClient.getMessage(true);
				Map<String, Object> map = JsonFactory.fromJson(message, Map.class);
				ids.add(map.get("id").toString());
				messages.add(map);
			}
			stopwatch.start();
			store.indexMessages(messages);
			stopwatch.stop();
		}
		long msgPerSec = (long) (100000.0 / (stopwatch.elapsed(TimeUnit.MILLISECONDS) / 1000.0));
		LOGGER.info(store.getClass().getName() + " : indexMessages : " + stopwatch.toString() + " - " + msgPerSec
				+ " msg/s");

		stopwatch.reset();
		for (int j = -5000; j < 100000; j++) {
			if (j == 0) {
				stopwatch.start();
			}
			store.containsId(ids.get(R.nextInt(ids.size())));
			store.containsId(UUID.randomUUID().toString());
		}
		stopwatch.stop();
		long idPerSec = (long) (200000.0 / (stopwatch.elapsed(TimeUnit.MILLISECONDS) / 1000.0));
		LOGGER.info(
				store.getClass().getName() + " : containsId : " + stopwatch.toString() + " - " + idPerSec + " ids/s");

		stopwatch.reset();
		for (int i = -50; i < 100; i++) {
			if (i == 0) {
				stopwatch.start();
			}
			store.getSummary();
		}
		stopwatch.stop();
		long synthesesPerSec = (long) (100.0 / (stopwatch.elapsed(TimeUnit.MILLISECONDS) / 1000.0));
		LOGGER.info(store.getClass().getName() + " : getSummary : " + stopwatch.toString() + " - " + synthesesPerSec
				+ " summaries/s");

	}

	private static Store getStoreES() {
		StoreElasticSearch store = new StoreElasticSearch();
		store.init(getTmpDossier());
		return store;
	}

	private static Store getStoreMapDB() {
		StoreMapDB store = new StoreMapDB();
		store.init(getTmpDossier());
		return store;
	}

	private static Store getStoreH2() {
		StoreH2 store = new StoreH2();
		store.init(getTmpDossier());
		return store;
	}

	private static String getTmpDossier() {
		try {
			File tmpFile = File.createTempFile("bench", "store");
			tmpFile.delete();
			return tmpFile.getAbsolutePath();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

}
