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
import com.capgemini.csd.hackaton.v2.store.StoreH2Mem;
import com.capgemini.csd.hackaton.v2.store.StoreH2Mem2;
import com.capgemini.csd.hackaton.v2.store.StoreMapDB;
import com.capgemini.csd.hackaton.v2.store.StoreObjectDB;
import com.google.common.base.Stopwatch;

public class StoreBench {

	private static final int N_MESSAGES = 1000;

	private static final int N_BATCH = 100;

	private static final int N_CONTAINSID = 100000;

	private static final int N_SYNTHESES = 1;

	public final static Logger LOGGER = LoggerFactory.getLogger(StoreBench.class);

	public final static Random R = new Random();

	public static void main(String[] args) {
		//		bench(getStoreES());
		//		bench(getStoreH2());
		bench(getStoreODB());
		bench(getStoreH2());
		bench(getStoreODB());
		//		bench(getStoreH2());
		//		bench(getStoreES());
		//		bench(getStoreH2Mem());
		//		bench(getStoreH2());
		//		bench(getStoreES());
		//		bench(getStoreH2Mem());
		//		bench(getStoreMapDB());
	}

	private static void bench(Store store) {
		Stopwatch stopwatch = Stopwatch.createUnstarted();
		store.getSummary(System.currentTimeMillis() - 3600 * 1000, 3600);
		List<String> ids = new ArrayList<>();
		for (int i = -N_BATCH / 5; i < N_BATCH; i++) {
			if (i == 0) {
				stopwatch = Stopwatch.createUnstarted();
			}
			List<Map<String, Object>> messages = new ArrayList<>(1001);
			for (int j = 0; j < N_MESSAGES; j++) {
				String message = AbstractClient.getMessage(true);
				Map<String, Object> map = JsonFactory.fromJson(message, Map.class);
				ids.add(map.get("id").toString());
				messages.add(map);
			}
			stopwatch.start();
			store.indexMessages(messages);
			stopwatch.stop();
		}
		long msgPerSec = (long) ((1.0 * N_BATCH * N_MESSAGES) / (stopwatch.elapsed(TimeUnit.MILLISECONDS) / 1000.0));
		LOGGER.info(store.getClass().getName() + " : indexMessages (" + N_BATCH + " * " + N_MESSAGES + ") : "
				+ stopwatch.toString() + " - " + msgPerSec + " msg/s");

		stopwatch.reset();
		for (int j = -N_CONTAINSID / 20; j < N_CONTAINSID; j++) {
			if (j == 0) {
				stopwatch.start();
			}
			if (!store.containsId(ids.get(R.nextInt(ids.size())))) {
				LOGGER.error("False negative");
			}
			if (store.containsId(UUID.randomUUID().toString())) {
				LOGGER.error("False positive");
			}
		}
		stopwatch.stop();
		long idPerSec = (long) ((2.0 * N_CONTAINSID) / (stopwatch.elapsed(TimeUnit.MILLISECONDS) / 1000.0));
		LOGGER.info(store.getClass().getName() + " : containsId (" + (2 * N_CONTAINSID) + ") : " + stopwatch.toString()
				+ " - " + idPerSec + " ids/s");

		stopwatch.reset();
		//		for (int i = -50; i < 100; i++) {
		for (int i = -N_SYNTHESES / 2; i < N_SYNTHESES; i++) {
			if (i == 0) {
				stopwatch.start();
			}
			store.getSummary(System.currentTimeMillis() - 3600 * 1000, 3600);
		}
		stopwatch.stop();
		long synthesesPerSec = (long) ((1.0 * N_SYNTHESES) / (stopwatch.elapsed(TimeUnit.MILLISECONDS) / 1000.0));
		LOGGER.info(store.getClass().getName() + " : getSummary (" + N_SYNTHESES + ") : " + stopwatch.toString() + " - "
				+ synthesesPerSec + " summaries/s");

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

	private static Store getStoreODB() {
		StoreObjectDB store = new StoreObjectDB();
		store.init(getTmpDossier());
		return store;
	}

	private static Store getStoreH2Mem() {
		StoreH2Mem store = new StoreH2Mem();
		store.init(getTmpDossier());
		return store;
	}

	private static Store getStoreH2Mem2() {
		StoreH2Mem2 store = new StoreH2Mem2();
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
