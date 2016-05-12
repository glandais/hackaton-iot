package com.capgemini.csd.hackaton.v2.queue.impl;

import java.util.LinkedList;

public class SimpleEventQueue<T> implements EventQueue<T> {

	private LinkedList<T> list = new LinkedList<T>();

	@Override
	public void put(T t) {
		list.addLast(t);
	}

	@Override
	public int size() {
		return list.size();
	}

	@Override
	public T take() {
		return list.removeFirst();
	}

}
