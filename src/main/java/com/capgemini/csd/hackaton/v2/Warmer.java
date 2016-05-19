package com.capgemini.csd.hackaton.v2;

import java.io.File;
import java.io.IOException;
import java.util.Calendar;
import java.util.Collections;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.capgemini.csd.hackaton.client.AbstractClient;
import com.capgemini.csd.hackaton.client.Client;
import com.capgemini.csd.hackaton.client.ClientAsyncHTTP;

public class Warmer {

	public final static Logger LOGGER = LoggerFactory.getLogger(Warmer.class);

	private static final int WARMUP_COUNT = 20000;

	public static void warmup(AbstractIOTServer abstractIOTServer) {
		LOGGER.info("Warming");
		int cacheSize = AbstractIOTServer.CACHE_SIZE;
		AbstractIOTServer.CACHE_SIZE = WARMUP_COUNT - 100;

		String dossierBackup = abstractIOTServer.getDossier();
		int portBackup = abstractIOTServer.getPort();
		abstractIOTServer.setDossier(getTmpDossier());
		abstractIOTServer.setPort(8080);
		abstractIOTServer.startServer(false);

		Client client = new ClientAsyncHTTP();
		client.razMessages();
		client.setHostPort("127.0.0.1", abstractIOTServer.getPort());
		LOGGER.info("Envoi de " + (WARMUP_COUNT / 2) + " messages en HTTP");
		client.sendMessages(WARMUP_COUNT / 2, true);
		LOGGER.info("Envoi de " + (WARMUP_COUNT / 2) + " messages en direct");
		for (int i = 0; i < WARMUP_COUNT / 2; i++) {
			try {
				abstractIOTServer.processRequest("/messages", Collections.emptyMap(), AbstractClient.getMessage(true));
			} catch (Exception e) {
				LOGGER.error(":(", e);
			}
		}
		Calendar start = Calendar.getInstance();
		start.add(Calendar.HOUR_OF_DAY, -1);
		for (int i = 0; i < 100; i++) {
			LOGGER.info("Demande de synthèse " + i);
			client.getSyntheseDistante(start.getTimeInMillis(), 3600 * 2);
		}
		LOGGER.info("Attente fin indexation");
		awaitWarmupTermination(abstractIOTServer);
		for (int i = 0; i < 100; i++) {
			LOGGER.info("Demande de synthèse " + i);
			client.getSyntheseDistante(start.getTimeInMillis(), 3600 * 2);
		}

		LOGGER.info("Fermeture");
		client.shutdown();
		abstractIOTServer.close();
		FileUtils.deleteQuietly(new File(abstractIOTServer.getDossier()));
		abstractIOTServer.setDossier(dossierBackup);
		abstractIOTServer.setPort(portBackup);

		AbstractIOTServer.CACHE_SIZE = cacheSize;
		System.gc();
		LOGGER.info("Warmed");
	}

	protected static void awaitWarmupTermination(AbstractIOTServer abstractIOTServer) {
		while (abstractIOTServer.getMemSize() != 0) {
			try {
				Thread.sleep(10L);
			} catch (InterruptedException e) {
				LOGGER.error(":(", e);
			}
		}
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
