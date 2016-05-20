package com.capgemini.csd.hackaton.v2;

import java.io.File;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
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

	private static final ExecutorService INDEX_EXECUTOR = Executors
			.newFixedThreadPool(Runtime.getRuntime().availableProcessors(), r -> {
				Thread thread = new Thread(r);
				thread.setPriority(Thread.MIN_PRIORITY);
				return thread;
			});

	private static final ExecutorService SUMMARY_EXECUTOR = Executors
			.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

	public static final boolean TEST_ID = false;

	public static int CACHE_SIZE = 1500000;

	private static final long SLEEP_PERSISTER = 5000L;

	private static final int BATCH_SIZE = 1001;

	public static final AtomicLong LAST_QUERY = new AtomicLong();

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

	public void setPort(int port) {
		this.port = port;
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
		index(false);

		startPersister();
	}

	@Override
	public String processRequest(String uri, Map<String, ? extends Collection<String>> params, String message)
			throws Exception {
		String result = "";
		LAST_QUERY.set(System.currentTimeMillis());
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
				index(false);
			}
		} catch (RuntimeException e) {
			LOGGER.error("", e);
			throw new Exception(e);
		}
		// LOGGER.info(uri + " " + params + " " + message + " -> " + result);
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
		Map<Integer, Summary> summary = new TreeMap<>();
		indexLock.readLock().lock();
		try {
			Future<Map<Integer, Summary>> storeFuture = SUMMARY_EXECUTOR
					.submit(() -> store.getSummary(timestamp, duration));
			Future<Map<Integer, Summary>> memFuture = SUMMARY_EXECUTOR
					.submit(() -> mem.getSummary(timestamp, duration));
			merge(summary, storeFuture.get());
			merge(summary, memFuture.get());
		} catch (Exception e) {
			LOGGER.error("", e);
		} finally {
			indexLock.readLock().unlock();
		}
		for (int i = 1; i < 11; i++) {
			if (!summary.containsKey(i)) {
				summary.put(i, new Summary(i, 1, 0, 0, 0));
			}
		}
		List<Summary> syntheses = new ArrayList<>(summary.values());
		String syntheseMaps = "[" + syntheses.stream().map(s -> s.toString()).collect(Collectors.joining(",")) + "]";
		return syntheseMaps;
	}

	protected void merge(Map<Integer, Summary> summary, Map<Integer, Summary> otherSummary) {
		for (Entry<Integer, Summary> entry : otherSummary.entrySet()) {
			Summary exist = summary.get(entry.getKey());
			if (exist != null) {
				exist.combine(entry.getValue());
			} else {
				summary.put(entry.getKey(), entry.getValue());
			}
		}
	}

	private void startPersister() {
		Thread thread = new Thread(() -> {
			while (true) {
				if (queue.getSize() > CACHE_SIZE
						|| (System.currentTimeMillis() - LAST_QUERY.get() > SLEEP_PERSISTER && queue.getSize() > 0)) {
					index(true);
				} else {
					Sys.sleep(SLEEP_PERSISTER);
				}
			}
		});
		thread.setName("persister");
		thread.setPriority(Thread.MIN_PRIORITY);
		thread.start();
	}

	private void index(boolean unSeulLot) {
		List<Message> messages = new ArrayList<>(BATCH_SIZE + 1);

		Message message = queue.readMessage();
		while (message != null) {
			messages.add(message);
			if (messages.size() == BATCH_SIZE) {
				indexMessages(messages);
				messages = new ArrayList<>(BATCH_SIZE + 1);
				if (unSeulLot) {
					break;
				}
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
			Future<?> storeFuture = INDEX_EXECUTOR.submit(() -> store.indexMessages(messages));
			Future<?> memFuture = INDEX_EXECUTOR.submit(() -> mem.removeMessages(messages));
			try {
				memFuture.get();
				storeFuture.get();
			} catch (Exception e) {
				LOGGER.error("erreur", e);
			}
		} finally {
			indexLock.writeLock().unlock();
		}
	}

	public long getQueueSize() {
		return queue.getSize();
	}
}
