package com.capgemini.csd.hackaton.server;

import java.io.IOException;

import org.boon.IO;
import org.restexpress.Request;
import org.restexpress.Response;
import org.restexpress.RestExpress;

import com.capgemini.csd.hackaton.Controler;
import com.capgemini.csd.hackaton.server.Server;

import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;

public class ServerRestExpress implements Server {

	private RestExpress server;

	public void start(Controler controler, int port) {
		server = new RestExpress().setName("IOT glandais");
		server.uri("/", new Object() {

			@SuppressWarnings("unused")
			public String create(Request req, Response res) throws IOException {
				return process(controler, req, res);
			}

			@SuppressWarnings("unused")
			public String read(Request req, Response res) throws IOException {
				return process(controler, req, res);
			}

			public String process(Controler controler, Request req, Response res) {
				try {
					String json = IO.read(req.getBodyAsStream());
					String result = controler.processRequest(req.getPath(), json);
					res.setResponseStatus(HttpResponseStatus.OK);
					return result;
				} catch (Exception e) {
					return error(res, e);
				}
			}

		}).method(HttpMethod.POST, HttpMethod.GET).noSerialization();
		server.bind(port);
	}

	@Override
	public void awaitTermination() {
		server.awaitShutdown();
	}

	@Override
	public void close() {
		server.shutdown();
	}

	protected String error(Response res, Exception e) {
		e.printStackTrace();
		res.setResponseStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
		return "Erreur 500 : " + e.toString();
	}
}
