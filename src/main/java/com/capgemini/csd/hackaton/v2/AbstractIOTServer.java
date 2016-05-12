package com.capgemini.csd.hackaton.v2;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.boon.core.Sys;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.capgemini.csd.hackaton.Controler;
import com.capgemini.csd.hackaton.Util;
import com.capgemini.csd.hackaton.client.Summary;
import com.capgemini.csd.hackaton.server.Server;
import com.capgemini.csd.hackaton.server.ServerNetty;
import com.capgemini.csd.hackaton.v2.mem.Mem;
import com.capgemini.csd.hackaton.v2.message.Message;
import com.capgemini.csd.hackaton.v2.message.Timestamp;
import com.capgemini.csd.hackaton.v2.store.Store;

import io.airlift.airline.Option;
import io.airlift.airline.OptionType;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ChronicleQueueBuilder;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.TailerDirection;

public abstract class AbstractIOTServer implements Runnable, Controler {

	public final static Logger LOGGER = LoggerFactory.getLogger(AbstractIOTServer.class);

	public static final boolean TEST_ID = false;

	public static int CACHE_SIZE = 1100000;

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
	protected ChronicleQueue queueToPersist;

	// blocage existance id
	protected ReentrantLock idLock = new ReentrantLock();

	// blocage indexation/calcul de la synthèse
	protected ReentrantReadWriteLock indexLock = new ReentrantReadWriteLock();

	protected abstract Mem getMem();

	protected abstract Store getStore();

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
		Warmer.warmup(this);
		startServer(true);
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				close();
			}
		});
	}

	public void configure() {
		dossier = new File(dossier).getAbsolutePath();
		LOGGER.info("Dossier : " + dossier);

		// liste de tous les messages
		queueToPersist = ChronicleQueueBuilder.single(dossier + "/queueToPersist").build();

		mem = getMem();
		store = getStore();

		// récupération de l'id du dernier message enregistré
		String lastPersistedId = getLastInsertedId();

		// enregistrement des messages non enregistrés
		ExcerptTailer tailerToPersist = queueToPersist.createTailer().direction(TailerDirection.BACKWARD).toEnd();
		index(tailerToPersist, lastPersistedId, true);

		queueToPersist.close();
		try {
			FileUtils.deleteDirectory(new File(dossier, "queueToPersist"));
		} catch (Exception e) {
			LOGGER.error("", e);
		}
		queueToPersist = ChronicleQueueBuilder.single(dossier + "/queueToPersist").build();

		startPersister();
	}

	public void startServer(boolean await) {
		configure();
		this.server = new ServerNetty();
		server.start(this, port);
		LOGGER.info("Serveur démarré");
		if (await) {
			server.awaitTermination();
		}
	}

	@Override
	public String processRequest(String uri, String message) throws Exception {
		String result = "";
		//		System.out.println(uri);
		try {
			if (uri.equals("/messages")) {
				process(message);
				result = "OK";
			} else if (uri.startsWith("/messages/synthesis?")) {
				Map<String, List<String>> params = Util.parse(uri.substring(uri.indexOf('?') + 1));
				List<String> ts = params.get("timestamp");
				long timestamp = 0;
				if (ts == null) {
					Calendar cal = Calendar.getInstance();
					cal.add(Calendar.HOUR, -1);
					timestamp = cal.getTimeInMillis();
				} else {
					timestamp = ISODateTimeFormat.dateTimeParser().parseMillis(ts.get(0));
				}
				List<String> durations = params.get("duration");
				int duration = 0;
				if (durations == null) {
					duration = 3600;
				} else {
					duration = Integer.valueOf(durations.get(0));
				}
				result = getSynthese(timestamp, duration);
			} else if (uri.equals("/index")) {
				index();
			}
		} catch (RuntimeException e) {
			LOGGER.error("", e);
			throw new Exception(e);
		}
		return result;
	}

	protected void index() {
		// noop
	}

	protected void close() {
		LOGGER.info("Fermeture");
		mem.close();
		server.close();
	}

	protected void process(String json) {
		Map<String, Object> message = Util.fromJson(json);
		String id = (String) message.get("id");

		if (TEST_ID) {
			idLock.lock();
			try {
				if (mem.containsId(id) || containsId(id)) {
					throw new RuntimeException("ID existant : " + id);
				}
				mem.putId(id);
			} finally {
				idLock.unlock();
			}
		}

		Timestamp ts = mem.index(message);
		// mise en queue pour la persistence
		writeMessage(message, id, ts);
	}

	protected void writeMessage(Map<String, Object> message, String id, Timestamp ts) {
		Long timestamp = (Long) message.get("timestamp");
		Integer sensorType = ((Number) message.get("sensorType")).intValue();
		Long value = ((Number) message.get("value")).longValue();

		ExcerptAppender appender = queueToPersist.createAppender();
		appender.writeDocument(w -> {
			w.write(() -> "timestamp").int64(timestamp);
			w.write(() -> "sensorType").int32(sensorType);
			w.write(() -> "value").int64(value);
			w.write(() -> "tsId").int32(ts.getId());
			w.write(() -> "id").text(id);
		});
	}

	protected Message readMessage(ExcerptTailer tailerToPersist) {
		Message[] messages = new Message[1];
		tailerToPersist.readDocument(r -> {
			long timestamp = r.read(() -> "timestamp").int64();
			int sensorType = r.read(() -> "sensorType").int32();
			long value = r.read(() -> "value").int64();
			int tsId = r.read(() -> "tsId").int32();
			String id = r.read(() -> "id").text();

			messages[0] = new Message(id, timestamp, sensorType, value, tsId);
		});
		return messages[0];
	}

	protected boolean containsId(String id) {
		return store.containsId(id);
	}

	protected String getSynthese(long timestamp, int duration) {
		indexLock.readLock().lock();
		try {
			Map<Integer, Summary> summarry = new TreeMap<>(getSummary(timestamp, duration));
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
			ExcerptTailer tailerToPersist = queueToPersist.createTailer();
			tailerToPersist.toEnd();
			while (true) {
				if (mem.getSize() > CACHE_SIZE) {
					index(tailerToPersist, null, false);
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

	private void index(ExcerptTailer tailerToPersist, String lastIndexed, boolean initial) {
		List<Message> messages = new ArrayList<>(BATCH_SIZE + 1);

		Message message = readMessage(tailerToPersist);
		Message precedent = null;
		if (initial && message != null) {
			setLastInsertedId(message.getId());
		}
		while (message != null) {
			if (lastIndexed != null && lastIndexed.equals(message.getId())) {
				break;
			}
			messages.add(message);
			if (messages.size() == BATCH_SIZE) {
				indexMessages(messages, initial);
				messages = new ArrayList<>(BATCH_SIZE + 1);
			}
			message = readMessage(tailerToPersist);
			if (message != null && precedent != null && message.getId().equals(precedent.getId())) {
				break;
			}
			precedent = message;
		}
		if (messages.size() > 0) {
			indexMessages(messages, initial);
		}
	}

	private void indexMessages(List<Message> messages, boolean initial) {
		LOGGER.info("Indexing " + messages.size() + " messages");
		indexLock.writeLock().lock();
		try {
			store.indexMessages(messages);
			mem.removeMessages(messages);
			if (!initial) {
				String id = (String) messages.get(messages.size() - 1).getId();
				setLastInsertedId(id);
			}
		} finally {
			indexLock.writeLock().unlock();
		}
	}

	protected File getLastInsertedFile() {
		return new File(dossier, "last");
	}

	private String getLastInsertedId() {
		if (getLastInsertedFile().exists()) {
			try {
				return FileUtils.readFileToString(getLastInsertedFile());
			} catch (IOException e) {
				LOGGER.error("", e);
				return null;
			}
		}
		return null;
	}

	private void setLastInsertedId(String id) {
		try {
			FileUtils.write(getLastInsertedFile(), id);
		} catch (IOException e) {
			LOGGER.error("", e);
		}
	}

	public long getMemSize() {
		return mem.getSize();
	}
}
