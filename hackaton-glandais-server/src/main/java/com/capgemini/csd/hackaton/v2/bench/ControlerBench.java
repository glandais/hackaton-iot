package com.capgemini.csd.hackaton.v2.bench;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.capgemini.csd.hackaton.Controler;
import com.capgemini.csd.hackaton.client.AbstractClient;
import com.capgemini.csd.hackaton.v2.AbstractIOTServer;
import com.capgemini.csd.hackaton.v2.IOTServerES;
import com.capgemini.csd.hackaton.v2.IOTServerH2;
import com.capgemini.csd.hackaton.v2.IOTServerH2ES;
import com.capgemini.csd.hackaton.v2.IOTServerH2Mem;
import com.capgemini.csd.hackaton.v2.IOTServerMapDB;
import com.capgemini.csd.hackaton.v2.IOTServerMem;
import com.capgemini.csd.hackaton.v2.IOTServerNoop;
import com.capgemini.csd.hackaton.v2.IOTServerODB;
import com.google.common.base.Stopwatch;

public class ControlerBench {

	private static final int MESSAGE_COUNT = 100000;

	private static final int MODULO_SYNTHESE = MESSAGE_COUNT * 2;

	public final static Logger LOGGER = LoggerFactory.getLogger(ControlerBench.class);

	public final static Random R = new Random();

	public static void main(String[] args) {
		//		bench(getNoop());
		//		bench(getMem());
		//				bench(getES());

		bench(getODB());
		bench(getH2());

		bench(getODB());
		bench(getH2());

		//		bench(getH2());

		//		bench(getH2ES());
		//		bench(getMapDB());
		System.exit(0);
	}

	private static void bench(Controler controler) {
		Stopwatch stopwatch = Stopwatch.createUnstarted();
		List<String> ids = new ArrayList<>();
		for (int i = -19999; i < MESSAGE_COUNT; i++) {
			if (i == 0) {
				stopwatch.start();
			}
			String messageId = AbstractClient.getMessageId();
			if (i % 1000 == 0) {
				messageId = ids.get(R.nextInt(ids.size()));
			} else {
				ids.add(messageId);
			}
			try {
				controler.processRequest("/messages", AbstractClient.getMessage(messageId, null, null, null));
				if (i % 1000 == 0) {
					//					LOGGER.error("Même id non détecté....................");
				}
			} catch (Exception e) {
				if (i % 1000 != 0) {
					//					LOGGER.error("Même id détecté....................", e);
				}
			}
			if (i % MODULO_SYNTHESE == 0) {
				try {
					controler.processRequest("/messages/synthesis", "");
				} catch (Exception e) {
					LOGGER.error("Erreur..........", e);
				}
			}
		}
		try {
			LOGGER.info(controler.processRequest("/messages/synthesis", ""));
		} catch (Exception e) {
			LOGGER.error("Erreur..........", e);
		}
		stopwatch.stop();

		if (controler instanceof AbstractIOTServer) {
			LOGGER.info("Memory used by mem : " + ((AbstractIOTServer) controler).mem.getMemorySize());
		}

		LOGGER.info(controler.getClass().getName() + " : total : " + stopwatch.toString());
		//		stopwatch.start();
		//		try {
		//			controler.processRequest("/index", "");
		//		} catch (Exception e) {
		//			LOGGER.error("Erreur..........", e);
		//		}
	}

	private static Controler getNoop() {
		IOTServerNoop controler = new IOTServerNoop();
		controler.setDossier(getTmpDossier());
		controler.configure();
		return controler;
	}

	private static Controler getMem() {
		IOTServerMem controler = new IOTServerMem();
		controler.setDossier(getTmpDossier());
		controler.configure();
		return controler;
	}

	private static Controler getMapDB() {
		IOTServerMapDB controler = new IOTServerMapDB();
		controler.setDossier(getTmpDossier());
		controler.configure();
		return controler;
	}

	private static Controler getH2ES() {
		IOTServerH2ES controler = new IOTServerH2ES();
		controler.setDossier(getTmpDossier());
		controler.configure();
		return controler;
	}

	private static Controler getH2() {
		IOTServerH2 controler = new IOTServerH2();
		controler.setDossier(getTmpDossier());
		controler.configure();
		return controler;
	}

	private static Controler getH2Mem() {
		IOTServerH2Mem controler = new IOTServerH2Mem();
		controler.setDossier(getTmpDossier());
		controler.configure();
		return controler;
	}

	private static Controler getODB() {
		IOTServerODB controler = new IOTServerODB();
		controler.setDossier(getTmpDossier());
		controler.configure();
		return controler;
	}

	private static Controler getES() {
		IOTServerES controler = new IOTServerES();
		controler.setDossier(getTmpDossier());
		controler.configure();
		return controler;
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
