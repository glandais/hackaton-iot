package com.capgemini.csd.hackaton.v2.queue;

import java.util.concurrent.ConcurrentLinkedQueue;

import com.capgemini.csd.hackaton.v2.message.Message;

public class QueueMem implements Queue {

	private ConcurrentLinkedQueue<Message> list = new ConcurrentLinkedQueue<Message>();

	@Override
	public Message readMessage() {
		return list.poll();
	}

	@Override
	public void put(Message mes) {
		list.offer(mes);
	}

	@Override
	public void init(String dossier) {
	}

	@Override
	public void close() {
	}

	@Override
	public long getSize() {
		return list.size();
	}

}
