package com.capgemini.csd.hackaton.server;

import org.boon.Exceptions;

import com.capgemini.csd.hackaton.Controler;

import io.undertow.Undertow;
import io.undertow.util.Headers;

public class ServerUndertow implements Server {

	private Undertow server;

	// private ExecutorService executor = Executors.newFixedThreadPool(20);

	@Override
	public void start(Controler controler, int port) {
		server = Undertow.builder().addHttpListener(port, "0.0.0.0").setHandler(ex -> {
			ex.getRequestReceiver().receiveFullString((ex2, message) -> {
				String uri = ex2.getRequestPath();
				String result = "";
				try {
					result = controler.processRequest(uri, ex2.getQueryParameters(), message);
				} catch (Exception e) {
					result = Exceptions.asJson(e);
					ex2.setStatusCode(500);
				}
				ex2.getResponseHeaders().put(Headers.CONTENT_TYPE, "application/json");
				ex2.getResponseSender().send(result);
			});
		}).build();
		server.start();
	}

	@Override
	public void awaitTermination() {

	}

	@Override
	public void close() {
		server.stop();
	}

}
