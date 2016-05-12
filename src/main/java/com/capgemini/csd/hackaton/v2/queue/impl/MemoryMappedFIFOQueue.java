package com.capgemini.csd.hackaton.v2.queue.impl;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

public class MemoryMappedFIFOQueue<T> implements EventQueue<T> {

	private FIFOByteArrayQueue queue;
	private Serializer<T> s;

	public MemoryMappedFIFOQueue(File f, Serializer<T> s, int fileSize) throws FileNotFoundException, IOException {
		this.queue = new FIFOByteArrayQueue(f, fileSize);
		this.s = s;
	}

	@Override
	public void put(T t) {
		byte[] checkpointData = s.toByteArray(t);
		queue.put(checkpointData);
	}

	@Override
	public int size() {
		return queue.size();
	}

	@Override
	public T take() {
		return s.interpret(queue.take());
	}

	public void createAndOpen() throws IOException {
		queue.createAndOpen();
	}

	public void shutdownAndSync() throws IOException {
		queue.shutdownAndSync();
	}

	public void shutdownAndDelete() throws IOException {
		queue.shutdownAndDelete();
	}

	public int remaining() {
		return queue.remaining();
	}

	public int getStart() {
		return queue.getStart();
	}

	public int getEnd() {
		return queue.getEnd();
	}

	public void reopen() throws IOException {
		queue.reopen();
	}

}
