package com.capgemini.csd.hackaton.v2.queue.impl;

public interface Serializer<T> {

    byte[] toByteArray(T t);
    
    T interpret(byte[] take);

}
