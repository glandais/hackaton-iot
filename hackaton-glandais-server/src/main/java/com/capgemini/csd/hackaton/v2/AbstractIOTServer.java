package com.capgemini.csd.hackaton.v2;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.boon.json.JsonFactory;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.capgemini.csd.hackaton.Controler;
import com.capgemini.csd.hackaton.Util;
import com.capgemini.csd.hackaton.client.AbstractClient;
import com.capgemini.csd.hackaton.client.Client;
import com.capgemini.csd.hackaton.client.ClientAsyncHTTP;
import com.capgemini.csd.hackaton.server.Server;
import com.capgemini.csd.hackaton.server.ServerNetty;
import com.capgemini.csd.hackaton.v2.mem.Mem;
import com.capgemini.csd.hackaton.v2.synthese.Summary;
import com.capgemini.csd.hackaton.v2.synthese.SyntheseJSON;

import io.airlift.airline.Option;
import io.airlift.airline.OptionType;
import net.openhft.chronicle.queue.ChronicleQueueBuilder;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;

public abstract class AbstractIOTServer implements Runnable, Controler {

	public final static Logger LOGGER = LoggerFactory.getLogger(AbstractIOTServer.class);

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

		Client client = new ClientAsyncHTTP();
		client.setHostPort("127.0.0.1", port);
		for (int i = 0; i < getWarmupMessageCount() / 2; i++) {
			client.sendMessage(true);
		}
		for (int i = 0; i < getWarmupMessageCount() / 2; i++) {
			try {
				processRequest("/messages", AbstractClient.getMessage(true));
			} catch (Exception e) {
				LOGGER.error(":(", e);
			}
		}
		client.getSynthese();
		awaitWarmupTermination();
		client.getSynthese();

		client.shutdown();
		close();
		FileUtils.deleteQuietly(new File(dossier));
		dossier = dossierTmp;
		LOGGER.info("Warmed");
	}

	protected void awaitWarmupTermination() {
		// noop
	}

	protected int getWarmupMessageCount() {
		//		return 100000;
		return 10;
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

		mem = getMem();

		init();
	}

	protected abstract void init();

	protected abstract Mem getMem();

	@Override
	public String processRequest(String uri, String message) throws Exception {
		String result = "";
		try {
			if (uri.equals("/messages")) {
				process(message);
				result = "OK";
			} else if (uri.startsWith("/messages/synthesis?")) {
				Map<String, List<String>> params = Util.parse(uri.substring(uri.indexOf('?') + 1));
				long timestamp = ISODateTimeFormat.dateTimeParser().parseMillis(params.get("timestamp").get(0));
				Integer duration = Integer.valueOf(params.get("duration").get(0));
				result = getSynthese(timestamp, duration);
			} else if (uri.equals("/index")) {
				index();
			}
		} catch (RuntimeException e) {
			throw new Exception(e);
		}
		return result;
	}

	protected void index() {
		// noop
	}

	protected void close() {
		LOGGER.info("Fermeture");
		server.close();
	}

	protected void process(String json) {
		Map<String, Object> message = JsonFactory.fromJson(json, Map.class);
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

		mem.index(message);

		// mise en queue pour la persistence
		queueToPersist.createAppender().writeText(json);
	}

	protected abstract boolean containsId(String id);

	protected String getSynthese(long timestamp, Integer duration) {
		indexLock.readLock().lock();
		try {
			Map<Integer, Summary> summarry = getSummary(timestamp, duration);
			List<Summary> syntheses = new ArrayList<>(summarry.values());
			List<Map<String, Object>> syntheseMaps = syntheses.stream().map(s -> s.toMap())
					.collect(Collectors.toList());
			return SyntheseJSON.getObjectMapper().toJson(syntheseMaps);
		} finally {
			indexLock.readLock().unlock();
		}
	}

	protected Map<Integer, Summary> getSummary(long timestamp, Integer duration) {
		return mem.getSummary(timestamp, duration);
	}

}
