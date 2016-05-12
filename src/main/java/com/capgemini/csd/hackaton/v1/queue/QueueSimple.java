package com.capgemini.csd.hackaton.v1.queue;

import java.util.concurrent.ConcurrentLinkedQueue;

public class QueueSimple implements Queue {

	private ConcurrentLinkedQueue<String> list;

	public QueueSimple() {
		super();
		this.list = new ConcurrentLinkedQueue<String>();
	}

	@Override
	public void push(String message) {
		this.list.offer(message);
	}

	@Override
	public String getMessage() {
		return list.poll();
	}

	@Override
	public void close() {

	}

	@Override
	public void init(String dossier) {

	}

}
