package com.capgemini.csd.hackaton.v2;

import java.io.File;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import org.boon.json.JsonFactory;
import org.boon.json.JsonParserFactory;
import org.boon.json.JsonSerializerFactory;
import org.boon.json.implementation.ObjectMapperImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.capgemini.csd.hackaton.Controler;
import com.capgemini.csd.hackaton.server.Server;
import com.capgemini.csd.hackaton.server.ServerNetty;
import com.capgemini.csd.hackaton.v2.mem.Mem;

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

	protected Mem mem;

	@Override
	public void run() {
		dossier = new File(dossier).getAbsolutePath();
		LOGGER.info("Dossier : " + dossier);

		// liste de tous les messages
		queueToPersist = ChronicleQueueBuilder.single(dossier + "/queueToPersist").build();

		mem = getMem();

		init();

		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				close();
			}
		});

		this.server = new ServerNetty();
		server.start(this, port);
		LOGGER.info("Serveur démarré");
		server.awaitTermination();
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
			} else if (uri.equals("/messages/synthesis")) {
				result = getSynthese();
			} else if (uri.equals("/index")) {
				index();
			}
		} catch (RuntimeException e) {
			LOGGER.error("?", e);
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

	protected String getSynthese() {
		indexLock.readLock().lock();
		try {
			Map<Integer, Summary> summarry = getSummary();
			List<Summary> syntheses = new ArrayList<>(summarry.values());
			List<Map<String, Object>> syntheseMaps = syntheses.stream().map(s -> s.toMap())
					.collect(Collectors.toList());
			JsonParserFactory jsonParserFactory = new JsonParserFactory();
			jsonParserFactory.lax();
			JsonSerializerFactory serializerFactory = new JsonSerializerFactory();
			serializerFactory.addTypeSerializer(BigDecimal.class, new BigDecimalSerializer());
			ObjectMapperImpl om = new ObjectMapperImpl(jsonParserFactory, serializerFactory);
			return om.toJson(syntheseMaps);
		} finally {
			indexLock.readLock().unlock();
		}
	}

	protected Map<Integer, Summary> getSummary() {
		return mem.getSummary();
	}

}
