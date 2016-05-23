package com.capgemini.csd.hackaton.v3;

import java.io.File;
import java.io.IOException;
import java.util.Calendar;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.joda.time.DateTime;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.capgemini.csd.hackaton.Controler;
import com.capgemini.csd.hackaton.Warmer;
import com.capgemini.csd.hackaton.server.Server;
import com.capgemini.csd.hackaton.server.ServerUndertow;
import com.capgemini.csd.hackaton.v3.messages.Message;
import com.squareup.moshi.JsonReader;

import io.airlift.airline.Command;
import io.airlift.airline.Option;
import io.airlift.airline.OptionType;
import okio.Buffer;

@Command(name = "server-v3", description = "Serveur V3")
public class IOTServerV3 implements Runnable, Controler {

	public final static Logger LOGGER = LoggerFactory.getLogger(IOTServerV3.class);

	// id incrémental, si deux messages avec le même timestamp
	private static final AtomicInteger currentId = new AtomicInteger();

	@Option(type = OptionType.GLOBAL, name = { "--port", "-p" }, description = "Port")
	protected int port = 80;

	@Option(type = OptionType.GLOBAL, name = { "--dossier", "-d" }, description = "Dossier")
	protected String dossier = "/var/glandais";

	// composant serveur
	protected Server server;

	protected Store store;

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
		Warmer.warmup(this);
		startServer(true);
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				close();
			}
		});
	}

	@Override
	public void configure() {
		dossier = new File(dossier).getAbsolutePath();
		LOGGER.info("Dossier : " + dossier);
		new File(dossier).mkdirs();
		this.store = new Store();
		this.store.init(dossier);
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

	@Override
	public String processRequest(String uri, Map<String, ? extends Collection<String>> params, String message)
			throws Exception {
		String result = "";
		try {
			if (uri.equals("/messages")) {
				process(message);
				result = "";
			} else if (uri.startsWith("/messages/synthesis")) {
				//				long start = System.nanoTime();
				Collection<String> ts = params.get("timestamp");
				long from = 0;
				if (ts == null) {
					Calendar cal = Calendar.getInstance();
					cal.add(Calendar.HOUR, -1);
					from = cal.getTimeInMillis();
				} else {
					from = ISODateTimeFormat.dateTimeParser().parseMillis(ts.iterator().next());
				}
				Collection<String> durations = params.get("duration");
				long to = 0;
				if (durations == null) {
					to = from + 1000 * 3600;
				} else {
					to = from + 1000 * Integer.valueOf(durations.iterator().next());
				}
				result = store.getSynthese(from, to).toString();
				// prendre au moins 1ms
				//				while (System.nanoTime() - start < 1000000)
				//					;
			}
		} catch (RuntimeException e) {
			LOGGER.error("", e);
			throw new Exception(e);
		}
		// LOGGER.info(uri + " " + params + " " + message + " -> " + result);
		return result;
	}

	@Override
	public void close() {
		LOGGER.info("Fermeture");
		if (server != null) {
			server.close();
		}
		if (store != null) {
			store.close();
		}
	}

	private void process(String json) {
		Buffer buffer = new Buffer();
		JsonReader jsonReader = JsonReader.of(buffer.writeUtf8(json));
		long timestamp = 0;
		int sensorType = 0;
		long value = 0;
		int idTs = currentId.getAndIncrement();
		try {
			jsonReader.beginObject();
			while (jsonReader.hasNext()) {
				String name = jsonReader.nextName();
				if (name.equals("timestamp")) {
					String tsFormatted = jsonReader.nextString();
					timestamp = DateTime.parse(tsFormatted).getMillis();
				} else if (name.equals("sensorType")) {
					sensorType = jsonReader.nextInt();
				} else if (name.equals("value")) {
					value = jsonReader.nextLong();
				} else {
					jsonReader.nextString();
				}
			}
			jsonReader.close();
		} catch (IOException e) {
		}
		buffer.close();
		store.process(new Message(timestamp, sensorType, value, idTs));
	}

	@Override
	public long getQueueSize() {
		return 0;
	}

}
