package org.hackaton.glandais.hackaton;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.boon.core.Sys;
import org.boon.json.JsonFactory;
import org.boon.json.ObjectMapper;
import org.hackaton.glandais.hackaton.server.Server;
import org.hackaton.glandais.hackaton.server.ServerNetty;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.airlift.airline.Cli;
import io.airlift.airline.Cli.CliBuilder;
import io.airlift.airline.Command;
import io.airlift.airline.Help;
import io.airlift.airline.Option;
import io.airlift.airline.OptionType;
import net.openhft.chronicle.queue.ChronicleQueueBuilder;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.TailerDirection;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;

@Command(name = "server", description = "Server")
public class App implements Controler, Runnable {

	public final static Logger LOGGER = LoggerFactory.getLogger(App.class);

	private static final int WARMUP_COUNT = 30000;

	private static final long SLEEP_PERSISTER = 5000L;

	private static final int CACHE_SIZE_REAL = 1100000;

	private static int CACHE_SIZE = CACHE_SIZE_REAL;

	private static final int BATCH_SIZE = 1000;

	protected Store store;

	@Option(type = OptionType.GLOBAL, name = { "--port", "-p" }, description = "Port")
	protected int port = 80;

	@Option(type = OptionType.GLOBAL, name = { "--dossier", "-d" }, description = "Dossier")
	protected String dossier = "/var/glandais";

	// composant serveur
	protected Server server;

	// queue des éléments à persisté
	protected SingleChronicleQueue queueToPersist;

	// blocage existance id
	protected ReentrantLock idLock = new ReentrantLock();

	// blocage indexation/calcul de la synthèse
	protected ReentrantReadWriteLock indexLock = new ReentrantReadWriteLock();

	protected ObjectMapper objectMapper = JsonFactory.createUseJSONDates();

	public static void main(String[] args) {
		CliBuilder<Runnable> builder = Cli.<Runnable> builder("hackaton-server").withDefaultCommand(Help.class)
				.withCommands(Help.class, App.class);

		Cli<Runnable> parser = builder.build();
		parser.parse(args).run();
	}

	@Override
	public void run() {
		warmup();

		configure();

		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				close();
			}
		});

		startServer(true);
	}

	public void setDossier(String dossier) {
		this.dossier = dossier;
	}

	public Mem mem;

	private void warmup() {
		LOGGER.info("Warming");
		String dossierTmp = dossier;
		dossier = getTmpDossier();
		configure();
		startServer(false);

		Client client = new Client();
		client.setHostPort("127.0.0.1", port);
		for (int i = 0; i < getWarmupMessageCount() / 2; i++) {
			client.sendMessage(true);
		}
		for (int i = 0; i < getWarmupMessageCount() / 2; i++) {
			try {
				processRequest("/messages", Util.getMessage(true));
			} catch (Exception e) {
				LOGGER.error(":(", e);
			}
		}
		Calendar start = Calendar.getInstance();
		start.add(Calendar.HOUR_OF_DAY, -1);
		client.getSynthese(start.getTime(), 3600 * 2);
		awaitWarmupTermination();
		client.getSynthese(start.getTime(), 3600 * 2);

		client.shutdown();
		close();
		FileUtils.deleteQuietly(new File(dossier));
		dossier = dossierTmp;
		LOGGER.info("Warmed");
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

	protected void startServer(boolean await) {
		this.server = new ServerNetty();
		server.start(this, port);
		LOGGER.info("Serveur démarré");
		if (await) {
			server.awaitTermination();
		}
	}

	public void configure() {
		dossier = new File(dossier).getAbsolutePath();
		LOGGER.info("Dossier : " + dossier);

		// liste de tous les messages
		queueToPersist = ChronicleQueueBuilder.single(dossier + "/queueToPersist").build();

		mem = new Mem();

		init();
	}

	public void init() {
		store = new Store();
		store.init(dossier);

		// récupération de l'id du dernier message enregistré
		String lastPersistedId = getLastInsertedId();

		// enregistrement des messages non enregistrés
		ExcerptTailer tailerToPersist = queueToPersist.createTailer().direction(TailerDirection.BACKWARD).toEnd();
		index(tailerToPersist, lastPersistedId, true);

		// FIXME RAZ queueToPersist quand tout a été persisté

		startPersister();
	}

	@Override
	public String processRequest(String uri, String message) throws Exception {
		String result = "";
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
			}
		} catch (RuntimeException e) {
			LOGGER.error("", e);
			throw new Exception(e);
		}
		return result;
	}

	protected void close() {
		LOGGER.info("Fermeture");
		server.close();
	}

	protected void process(String json) {
		Map<String, Object> message = Util.fromJson(json);
		String id = (String) message.get("id");

		idLock.lock();
		try {
			if (mem.containsId(id) || containsId(id)) {
				throw new RuntimeException("ID existant : " + id);
			}
			mem.putId(id);
		} finally {
			idLock.unlock();
		}

		UUID uuid = mem.index(message);
		// mise en queue pour la persistence
		writeMessage(message, id, uuid);
	}

	protected void writeMessage(Map<String, Object> message, String id, UUID uuid) {
		Long timestamp = ((Date) message.get("timestamp")).getTime();
		Integer sensorType = ((Number) message.get("sensorType")).intValue();
		Long value = ((Number) message.get("value")).longValue();

		ExcerptAppender appender = queueToPersist.createAppender();
		appender.writeDocument(w -> {
			w.write(() -> "timestamp").int64(timestamp);
			w.write(() -> "sensorType").int32(sensorType);
			w.write(() -> "value").int64(value);
			w.write(() -> "uuidMost").int64(uuid.getMostSignificantBits());
			w.write(() -> "uuidLeast").int64(uuid.getLeastSignificantBits());
			w.write(() -> "id").text(id);
		});
	}

	private Message readMessage(ExcerptTailer tailerToPersist) {
		Message[] messages = new Message[1];
		tailerToPersist.readDocument(r -> {
			long timestamp = r.read(() -> "timestamp").int64();
			int sensorType = r.read(() -> "sensorType").int32();
			long value = r.read(() -> "value").int64();
			long uuidMost = r.read(() -> "uuidMost").int64();
			long uuidLeast = r.read(() -> "uuidLeast").int64();
			UUID uuid = new UUID(uuidMost, uuidLeast);
			String id = r.read(() -> "id").text();

			messages[0] = new Message(id, new Date(timestamp), sensorType, value, uuid);
		});
		return messages[0];
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

	protected void awaitWarmupTermination() {
		while (mem.getSize() != 0) {
			try {
				Thread.sleep(10L);
			} catch (InterruptedException e) {
				LOGGER.error(":(", e);
			}
		}
		CACHE_SIZE = CACHE_SIZE_REAL;
	}

	protected int getWarmupMessageCount() {
		CACHE_SIZE = WARMUP_COUNT - 100;
		return WARMUP_COUNT;
	}

	protected boolean containsId(String id) {
		return store.containsId(id);
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
}
