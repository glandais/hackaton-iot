package com.capgemini.csd.hackaton.v1.queue;

public class QueueNoop implements Queue {

	@Override
	public void push(String message) {
	}

	@Override
	public String getMessage() {
		return null;
	}

	@Override
	public void close() {
	}

	@Override
	public void init(String dossier) {
	}

}
