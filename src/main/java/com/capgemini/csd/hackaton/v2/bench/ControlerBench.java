package com.capgemini.csd.hackaton.v2.bench;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Random;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.capgemini.csd.hackaton.Controler;
import com.capgemini.csd.hackaton.client.AbstractClient;
import com.capgemini.csd.hackaton.v2.IOTServerLucene;
import com.capgemini.csd.hackaton.v2.IOTServerMem;
import com.capgemini.csd.hackaton.v2.IOTServerNoop;
import com.capgemini.csd.hackaton.v2.IOTServerODB;
import com.google.common.base.Stopwatch;

import io.airlift.airline.Command;
import io.airlift.airline.Option;
import io.airlift.airline.OptionType;

@Command(name = "bench-controler", description = "Bench controler")
public class ControlerBench implements Runnable {

	private static final int MESSAGE_COUNT = 1000000;

	private static final int MODULO_SYNTHESE = MESSAGE_COUNT * 2;

	public final static Logger LOGGER = LoggerFactory.getLogger(ControlerBench.class);

	public final static Random R = new Random();

	@Option(type = OptionType.GLOBAL, name = { "-noop" }, description = "Noop")
	protected boolean noop = false;

	@Option(type = OptionType.GLOBAL, name = { "-mem" }, description = "Mem")
	protected boolean mem = false;

	@Option(type = OptionType.GLOBAL, name = { "-lucene" }, description = "Lucene")
	protected boolean lucene = false;

	@Option(type = OptionType.GLOBAL, name = { "-odb" }, description = "ODB")
	protected boolean odb = false;

	public static void main(String[] args) {
		ControlerBench controlerBench = new ControlerBench();
		controlerBench.noop = true;
		controlerBench.mem = true;
		controlerBench.lucene = true;
		controlerBench.odb = true;
		controlerBench.run();
	}

	@Override
	public void run() {
		if (lucene) {
			bench(new IOTServerLucene());
		}
		if (noop) {
			bench(new IOTServerNoop());
		}
		if (mem) {
			bench(new IOTServerMem());
		}
		if (odb) {
			bench(new IOTServerODB());
		}
		System.exit(0);
	}

	private void bench(Controler controler) {
		String dossier = getTmpDossier();
		controler.setDossier(dossier);
		controler.configure();

		Stopwatch stopwatch = Stopwatch.createUnstarted();
		// List<String> ids = new ArrayList<>();
		for (int i = -19999; i < MESSAGE_COUNT; i++) {
			if (i == 0) {
				stopwatch.start();
			}
			String messageId = AbstractClient.getMessageId();
			// if (i % 1000 == 0) {
			// messageId = ids.get(R.nextInt(ids.size()));
			// } else {
			// ids.add(messageId);
			// }
			try {
				controler.processRequest("/messages", Collections.emptyMap(),
						AbstractClient.getMessage(messageId, null, null, null));
				if (i % 1000 == 0) {
					// LOGGER.error("Même id non détecté....................");
				}
			} catch (Exception e) {
				if (i % 1000 != 0) {
					// LOGGER.error("Même id détecté....................", e);
				}
			}
			if (i % MODULO_SYNTHESE == 0) {
				try {
					controler.processRequest("/messages/synthesis", Collections.emptyMap(), "");
				} catch (Exception e) {
					LOGGER.error("Erreur..........", e);
				}
			}
		}
		try {
			LOGGER.info(controler.processRequest("/messages/synthesis", Collections.emptyMap(), ""));
		} catch (Exception e) {
			LOGGER.error("Erreur..........", e);
		}
		stopwatch.stop();

		LOGGER.info(controler.getClass().getName() + " : total : " + stopwatch.toString());
		controler.close();
		try {
			FileUtils.deleteDirectory(new File(dossier));
		} catch (IOException e) {
			LOGGER.error("", e);
		}
	}

	private String getTmpDossier() {
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
