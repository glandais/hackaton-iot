package com.capgemini.csd.hackaton.v2;

import java.io.File;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import org.boon.core.Sys;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.capgemini.csd.hackaton.Controler;
import com.capgemini.csd.hackaton.Util;
import com.capgemini.csd.hackaton.client.Summary;
import com.capgemini.csd.hackaton.server.Server;
import com.capgemini.csd.hackaton.server.ServerUndertow;
import com.capgemini.csd.hackaton.v2.mem.Mem;
import com.capgemini.csd.hackaton.v2.message.Message;
import com.capgemini.csd.hackaton.v2.queue.Queue;
import com.capgemini.csd.hackaton.v2.store.Store;

import io.airlift.airline.Option;
import io.airlift.airline.OptionType;

public abstract class AbstractIOTServer implements Runnable, Controler {

	public final static Logger LOGGER = LoggerFactory.getLogger(AbstractIOTServer.class);

	public static final boolean TEST_ID = false;

	public static int CACHE_SIZE = 9999999;

	private static final long SLEEP_PERSISTER = 5000L;

	private static final int BATCH_SIZE = 1000;

	@Option(type = OptionType.GLOBAL, name = { "--port", "-p" }, description = "Port")
	protected int port = 80;

	@Option(type = OptionType.GLOBAL, name = { "--dossier", "-d" }, description = "Dossier")
	protected String dossier = "/var/glandais";

	// composant serveur
	protected Server server;

	protected Mem mem;

	protected Store store;

	// queue des éléments à persisté
	protected Queue queue;

	// blocage existance id
	protected ReentrantLock idLock = new ReentrantLock();

	// blocage indexation/calcul de la synthèse
	protected ReentrantReadWriteLock indexLock = new ReentrantReadWriteLock();

	protected abstract Mem getMem();

	protected abstract Store getStore();

	protected abstract Queue getQueue();

	public int getPort() {
		return port;
	}

	public String getDossier() {
		return dossier;
	}

	public void setDossier(String dossier) {
		this.dossier = dossier;
	}

	@Override
	public void run() {
		warmup();
		startServer(true);
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				close();
			}
		});
	}

	protected void warmup() {
		Warmer.warmup(this);
	}

	public void startServer(boolean await) {
		configure();
		this.server = new ServerUndertow();
		server.start(this, port);
		LOGGER.info("Serveur démarré");
		if (await) {
			server.awaitTermination();
		}
	}

	public void configure() {
		dossier = new File(dossier).getAbsolutePath();
		LOGGER.info("Dossier : " + dossier);
		new File(dossier).mkdirs();

		// liste de tous les messages
		queue = getQueue();
		queue.init(dossier);

		mem = getMem();
		store = getStore();
		store.init(dossier);

		// enregistrement des messages non enregistrés
		index();

		startPersister();
	}

	@Override
	public String processRequest(String uri, Map<String, ? extends Collection<String>> params, String message)
			throws Exception {
		String result = "";
		try {
			if (uri.equals("/messages")) {
				process(message);
				result = "";
			} else if (uri.startsWith("/messages/synthesis")) {
				Collection<String> ts = params.get("timestamp");
				long timestamp = 0;
				if (ts == null) {
					Calendar cal = Calendar.getInstance();
					cal.add(Calendar.HOUR, -1);
					timestamp = cal.getTimeInMillis();
				} else {
					timestamp = ISODateTimeFormat.dateTimeParser().parseMillis(ts.iterator().next());
				}
				Collection<String> durations = params.get("duration");
				int duration = 0;
				if (durations == null) {
					duration = 3600;
				} else {
					duration = Integer.valueOf(durations.iterator().next());
				}
				result = getSynthese(timestamp, duration);
			} else if (uri.equals("/index")) {
				index();
			}
		} catch (RuntimeException e) {
			LOGGER.error("", e);
			throw new Exception(e);
		}
		//		LOGGER.info(uri + " " + params + " " + message + " -> " + result);
		return result;
	}

	public void close() {
		LOGGER.info("Fermeture");
		if (server != null) {
			server.close();
		}
		mem.close();
		queue.close();
		store.close();
	}

	protected void process(String json) {
		Message message = Util.messageFromJson(json);
		String id = message.getId();

		if (TEST_ID) {
			idLock.lock();
			try {
				if (mem.containsId(id) || store.containsId(id)) {
					throw new RuntimeException("ID existant : " + id);
				}
				mem.putId(id);
			} finally {
				idLock.unlock();
			}
		}

		mem.index(message);
		// mise en queue pour la persistence
		queue.put(message);
	}

	protected String getSynthese(long timestamp, int duration) {
		indexLock.readLock().lock();
		try {
			Map<Integer, Summary> summarry = new TreeMap<>(getSummary(timestamp, duration));
			for (int i = 1; i < 11; i++) {
				if (!summarry.containsKey(i)) {
					summarry.put(i, new Summary(i, 1, 0, 0, 0));
				}
			}
			List<Summary> syntheses = new ArrayList<>(summarry.values());
			String syntheseMaps = "[" + syntheses.stream().map(s -> s.toString()).collect(Collectors.joining(","))
					+ "]";
			return syntheseMaps;
		} finally {
			indexLock.readLock().unlock();
		}
	}

	protected Map<Integer, Summary> getSummary(long timestamp, Integer duration) {
		Map<Integer, Summary> storeSummary = store.getSummary(timestamp, duration);
		Map<Integer, Summary> memSummary = mem.getSummary(timestamp, duration);
		Map<Integer, Summary> summary = new HashMap<Integer, Summary>();

		for (Entry<Integer, Summary> entry : storeSummary.entrySet()) {
			if (memSummary.containsKey(entry.getKey())) {
				entry.getValue().combine(memSummary.get(entry.getKey()));
			}
			summary.put(entry.getKey(), entry.getValue());
		}
		for (Entry<Integer, Summary> entry : memSummary.entrySet()) {
			if (!storeSummary.containsKey(entry.getKey())) {
				summary.put(entry.getKey(), entry.getValue());
			}
		}
		return summary;
	}

	private void startPersister() {
		Thread thread = new Thread(() -> {
			while (true) {
				if (mem.getSize() > CACHE_SIZE) {
					index();
					Sys.sleep(1L);
				} else {
					Sys.sleep(SLEEP_PERSISTER);
				}
			}
		});
		thread.setName("persister");
		thread.setPriority(Thread.MIN_PRIORITY);
		thread.start();
	}

	private void index() {
		List<Message> messages = new ArrayList<>(BATCH_SIZE + 1);

		Message message = queue.readMessage();
		while (message != null) {
			messages.add(message);
			if (messages.size() == BATCH_SIZE) {
				indexMessages(messages);
				messages = new ArrayList<>(BATCH_SIZE + 1);
			}
			message = queue.readMessage();
		}
		if (messages.size() > 0) {
			indexMessages(messages);
		}
	}

	private void indexMessages(List<Message> messages) {
		LOGGER.info("Indexing " + messages.size() + " messages");
		indexLock.writeLock().lock();
		try {
			store.indexMessages(messages);
			mem.removeMessages(messages);
		} finally {
			indexLock.writeLock().unlock();
		}
	}

	public long getMemSize() {
		return mem.getSize();
	}
}
