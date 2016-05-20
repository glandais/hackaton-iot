package com.capgemini.csd.hackaton.v2.queue;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.capgemini.csd.hackaton.v2.message.Message;
import com.capgemini.csd.hackaton.v2.message.MessageSerializer;
import com.capgemini.csd.hackaton.v2.queue.impl.MemoryMappedFIFOQueue;

public class QueueSpiderPig implements Queue {

	protected MemoryMappedFIFOQueue<Message> queue;

	protected ReentrantReadWriteLock queueLock = new ReentrantReadWriteLock();

	@Override
	public void init(String dossier) {
		try {
			File file = new File(dossier, "queueToPersist");
			queue = new MemoryMappedFIFOQueue<Message>(file, new MessageSerializer(), 256 * 1024 * 1024);
			if (!file.exists()) {
				queue.createAndOpen();
			} else {
				queue.reopen();
			}
		} catch (Exception e) {
			throw new IllegalStateException("", e);
		}
	}

	@Override
	public Message readMessage() {
		queueLock.readLock().lock();
		try {
			if (queue.size() > 0) {
				return queue.take();
			} else {
				return null;
			}
		} finally {
			queueLock.readLock().unlock();
		}
	}

	@Override
	public void put(Message mes) {
		queueLock.writeLock().lock();
		try {
			queue.put(mes);
		} finally {
			queueLock.writeLock().unlock();
		}
	}

	@Override
	public void close() {
		try {
			queue.shutdownAndSync();
		} catch (IOException e) {
			throw new IllegalStateException("", e);
		}
	}

	@Override
	public long getSize() {
		return queue.size();
	}

}
