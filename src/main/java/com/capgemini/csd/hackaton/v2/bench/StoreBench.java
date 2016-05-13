package com.capgemini.csd.hackaton.v2.bench;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.capgemini.csd.hackaton.Util;
import com.capgemini.csd.hackaton.client.AbstractClient;
import com.capgemini.csd.hackaton.v2.message.Message;
import com.capgemini.csd.hackaton.v2.store.Store;
import com.capgemini.csd.hackaton.v2.store.StoreLucene;
import com.capgemini.csd.hackaton.v2.store.StoreNoop;
import com.capgemini.csd.hackaton.v2.store.StoreObjectDB;
import com.google.common.base.Stopwatch;

import io.airlift.airline.Command;
import io.airlift.airline.Option;
import io.airlift.airline.OptionType;

@Command(name = "bench-store", description = "Bench store")
public class StoreBench implements Runnable {

	private static final int N_MESSAGES = 1000;

	private static final int N_BATCH = 100;

	private static final int N_CONTAINSID = 100000;

	private static final int N_SYNTHESES = 10;

	public final static Logger LOGGER = LoggerFactory.getLogger(StoreBench.class);

	public final static Random R = new Random();

	@Option(type = OptionType.GLOBAL, name = { "-noop" }, description = "Noop")
	protected boolean noop = false;

	@Option(type = OptionType.GLOBAL, name = { "-odb" }, description = "ODB")
	protected boolean odb = false;

	@Option(type = OptionType.GLOBAL, name = { "-lucene" }, description = "Lucene")
	protected boolean lucene = false;

	public static void main(String[] args) {
		StoreBench storeBench = new StoreBench();
		//		storeBench.noop = true;
		storeBench.odb = true;
		storeBench.lucene = true;
		storeBench.run();
	}

	@Override
	public void run() {
		if (lucene) {
			bench(new StoreLucene(512));
		}
		if (noop) {
			bench(new StoreNoop());
		}
		if (odb) {
			bench(new StoreObjectDB());
		}
	}

	private static void bench(Store store) {
		String tmpDossier = getTmpDossier();
		new File(tmpDossier).mkdirs();
		store.init(tmpDossier);
		Stopwatch stopwatch = Stopwatch.createUnstarted();
		store.getSummary(System.currentTimeMillis() - 3600 * 1000, 3600);
		List<String> ids = new ArrayList<>();
		for (int i = -N_BATCH / 5; i < N_BATCH; i++) {
			if (i == 0) {
				stopwatch = Stopwatch.createUnstarted();
			}
			List<Message> messages = new ArrayList<>(1001);
			for (int j = 0; j < N_MESSAGES; j++) {
				Message message = Util.messageFromJson(AbstractClient.getMessage(true));
				ids.add(message.getId());
				messages.add(message);
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
			store.getSummary(System.currentTimeMillis() - 3600 * 1000, 3700);
		}
		stopwatch.stop();
		long synthesesPerSec = (long) ((1.0 * N_SYNTHESES) / (stopwatch.elapsed(TimeUnit.MILLISECONDS) / 1000.0));
		LOGGER.info(store.getClass().getName() + " : getSummary (" + N_SYNTHESES + ") : " + stopwatch.toString() + " - "
				+ synthesesPerSec + " summaries/s");

		store.close();

		try {
			FileUtils.deleteDirectory(new File(tmpDossier));
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	private static String getTmpDossier() {
		try {
			File tmpFile = File.createTempFile("bench", "store");
			tmpFile.delete();
			tmpFile.mkdirs();
			return tmpFile.getAbsolutePath();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

}
