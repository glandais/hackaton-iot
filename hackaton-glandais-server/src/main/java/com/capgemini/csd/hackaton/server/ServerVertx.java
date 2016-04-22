package com.capgemini.csd.hackaton.server;

import java.io.IOException;

import org.boon.Exceptions;

import com.capgemini.csd.hackaton.Controler;

import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.http.HttpServer;

public class ServerVertx implements Server {

	private HttpServer httpServer;

	@Override
	public void start(Controler controler, int port) {
		httpServer = Vertx.vertx(new VertxOptions()).createHttpServer();
		httpServer.requestHandler(e -> e.bodyHandler(h -> {
			try {
				e.response().end(controler.processRequest(e.absoluteURI(), h.toString()));
			} catch (Exception e1) {
				e.response().setStatusCode(500);
				e.response().setStatusMessage(Exceptions.asJson(e1));
			}
		})).listen(port);

	}

	@Override
	public void awaitTermination() {
		try {
			System.in.read();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void close() {
		httpServer.close();
	}

}
