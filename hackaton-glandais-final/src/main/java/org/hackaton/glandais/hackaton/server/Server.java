package org.hackaton.glandais.hackaton.server;

import org.hackaton.glandais.hackaton.Controler;

public interface Server {

	void start(Controler controler, int port);

	void awaitTermination();

	void close();

}