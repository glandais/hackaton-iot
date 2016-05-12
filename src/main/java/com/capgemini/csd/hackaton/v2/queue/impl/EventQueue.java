package com.capgemini.csd.hackaton.v2.queue.impl;

public interface EventQueue<T> {

    public void put(T t);

    public T take();

    public int size();
    
}
