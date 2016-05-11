package org.hackaton.glandais.hackaton;

import java.io.IOException;
import java.util.Date;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.AsyncHttpClientConfig;
import org.asynchttpclient.DefaultAsyncHttpClient;
import org.asynchttpclient.DefaultAsyncHttpClientConfig;
import org.asynchttpclient.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Client {

	public final static Logger LOGGER = LoggerFactory.getLogger(Client.class);

	private ExecutorService executor;

	protected String host;

	protected int port;

	private final AsyncHttpClient httpClient;

	public Client() {
		super();
		int threads = 10;
		LOGGER.info("Initialisation avec " + threads + " threads.");
		executor = Executors.newFixedThreadPool(threads);
		AsyncHttpClientConfig cf = new DefaultAsyncHttpClientConfig.Builder().setKeepAlive(true).build();
		httpClient = new DefaultAsyncHttpClient(cf);
	}

	public void setHostPort(String host, int port) {
		this.host = host;
		this.port = port;
	}

	public void sendMessage(boolean randomTime) {
		sendMessage(Util.getMessage(randomTime));
	}

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

	public void sendMessage(String message) {
		try {
			//			LOGGER.info("Sending " + message);
			Response response = httpClient.preparePost("http://" + host + ":" + port + "/messages").setBody(message)
					.execute().get();
			//			LOGGER.info("Response " + response.getStatusCode());
			if (response.getStatusCode() != 200) {
				throw new IllegalStateException("Erreur! " + response);
			}
		} catch (Exception e) {
			LOGGER.error("Echec à l'envoi du message", e);
		}
	}

	public String getSynthese(Date start, int duration) {
		try {
			//			LOGGER.info("Sending getSynthese");
			String response = httpClient.prepareGet("http://" + host + ":" + port + "/messages/synthesis")
					.addQueryParam("timestamp", Util.getMessageTimestamp(start))
					.addQueryParam("duration", duration + "").execute().get().getResponseBody();
			//			LOGGER.info("Response " + response);
			return response;
		} catch (Exception e) {
			LOGGER.error("Echec", e);
			return "";
		}
	}

	public void shutdown() {
		LOGGER.info("Fermeture du pool d'exécution.");
		executor.shutdownNow();
		try {
			httpClient.close();
		} catch (IOException e) {
			LOGGER.error("Erreur", e);
		}
	}

}
