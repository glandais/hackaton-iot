package com.capgemini.csd.hackaton.server;

import com.capgemini.csd.hackaton.Controler;

public interface Server {

	void start(Controler controler, int port);

	void awaitTermination();

	void close();

}
