package com.capgemini.csd.hackaton.client;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractClient implements Client {

	public final static Logger LOGGER = LoggerFactory.getLogger(AbstractClient.class);

	protected static final int SENSOR_TYPES = 10;

	private static final ThreadLocal<Random> r = ThreadLocal.withInitial(() -> new Random());

	protected static final ThreadLocal<DateFormat> dateFormat = ThreadLocal.withInitial(() -> {
		// Use RFC3339 format for date and datetime.
		// See http://xml2rfc.ietf.org/public/rfc/html/rfc3339.html#anchor14
		DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
		// Use UTC as the default time zone.
		dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
		return dateFormat;
	});

	public static String getMessage(boolean randomTime) {
		return getMessage(null, randomTime ? null : new Date(), null, null);
	}

	public static String getMessage(String messageId, Date date, Integer sensorType, Long value) {
		return "{ \"id\" : \"" + (messageId == null ? getMessageId() : messageId) + "\", \"timestamp\" : \""
				+ (date == null ? getMessageTimestamp() : getMessageTimestamp(date)) + "\", \"sensorType\" : "
				+ (sensorType == null ? getMessageSensorType().toString() : sensorType) + ", \"value\" : "
				+ (value == null ? getMessageValue().toString() : value) + "}";
	}

	public static String getMessageId() {
		return getUUID() + getUUID();
	}

	protected static String getUUID() {
		return UUID.randomUUID().toString().replaceAll("-", "");
	}

	public static Integer getMessageSensorType() {
		return r.get().nextInt(SENSOR_TYPES);
	}

	public static String getMessageTimestamp() {
		return getMessageTimestamp(new Date(new Date().getTime() - 10000 + r.get().nextInt(20000)));
	}

	public static String getMessageTimestamp(Date date) {
		return dateFormat.get().format(date);
	}

	public static Long getMessageValue() {
		//		return r.get().nextInt(10000);
		return r.get().nextLong();
	}

	private ExecutorService executor;

	protected String host;

	protected int port;

	public AbstractClient() {
		super();
		int threads = 2 * Runtime.getRuntime().availableProcessors();
		LOGGER.info("Initialisation avec " + threads + " threads.");
		executor = Executors.newFixedThreadPool(threads);
	}

	@Override
	public void setHostPort(String host, int port) {
		this.host = host;
		this.port = port;
	}

	@Override
	public void sendMessage(boolean randomTime) {
		sendMessage(getMessage(randomTime));
	}

	@Override
	public void sendMessages(int count, boolean randomTime) {
		LOGGER.info("Envoi de " + count + " messages.");
		Future<?>[] futures = new Future<?>[count];
		long start = System.nanoTime();
		for (int i = 0; i < count; i++) {
			futures[i] = executor.submit(() -> sendMessage(randomTime));
		}
		for (Future<?> future : futures) {
			try {
				future.get();
			} catch (InterruptedException | ExecutionException e) {
				LOGGER.error("?", e);
			}
		}
		long end = System.nanoTime();
		double diff = (end - start) / 1000000000.0;
		double rate = count / diff;
		LOGGER.info(rate + " messages/s");
	}

	public void shutdown() {
		LOGGER.info("Fermeture du pool d'exécution.");
		executor.shutdownNow();
	}

}
