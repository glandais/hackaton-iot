package com.capgemini.csd.hackaton.v1.queue;

public interface Queue {

	void push(String message);

	String getMessage();

	void close();

	void init(String dossier);

}
