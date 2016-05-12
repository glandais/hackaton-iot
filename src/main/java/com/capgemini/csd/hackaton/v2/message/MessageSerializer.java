package com.capgemini.csd.hackaton.v2.message;

import org.nustaq.serialization.FSTConfiguration;

import com.capgemini.csd.hackaton.v2.queue.impl.Serializer;

public class MessageSerializer implements Serializer<Message> {

	private static ThreadLocal<FSTConfiguration> conf = ThreadLocal
			.withInitial(() -> FSTConfiguration.createDefaultConfiguration());

	@Override
	public byte[] toByteArray(Message t) {
		return conf.get().asByteArray(t);
	}

	@Override
	public Message interpret(byte[] take) {
		return (Message) conf.get().asObject(take);
	}

}
