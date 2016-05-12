package com.capgemini.csd.hackaton.v2.queue.impl;

public class StringSerializer implements Serializer<String> {

    @Override
    public byte[] toByteArray(String t) {
        return t.getBytes();
    }

    @Override
    public String interpret(byte[] take) {
        return new String(take);
    }

}
