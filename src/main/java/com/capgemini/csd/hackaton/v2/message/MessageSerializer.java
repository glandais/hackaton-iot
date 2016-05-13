package com.capgemini.csd.hackaton.v2.message;

import java.nio.ByteBuffer;

import com.capgemini.csd.hackaton.v2.queue.impl.Serializer;

public class MessageSerializer implements Serializer<Message> {

	private static ThreadLocal<ByteBuffer> bytesTL = ThreadLocal.withInitial(() -> {
		ByteBuffer res = ByteBuffer.allocate(92);
		res.mark();
		return res;
	});

	@Override
	public byte[] toByteArray(Message t) {
		ByteBuffer bytes = bytesTL.get();
		bytes.reset();
		bytes.putLong(t.getTimestamp());
		bytes.putInt(t.getIdTs());
		bytes.putInt(t.getSensorType());
		bytes.putLong(t.getValue());
		String id = t.getId();
		int l = id.length();
		bytes.putInt(l);
		for (int i = 0; i < l; i++) {
			bytes.put((byte) id.charAt(i));
		}
		return bytes.array();
	}

	@Override
	public Message interpret(byte[] take) {
		ByteBuffer bytes = ByteBuffer.wrap(take);
		long timestamp = bytes.getLong();
		int idTs = bytes.getInt();
		int sensorType = bytes.getInt();
		long value = bytes.getLong();
		int l = bytes.getInt();
		char[] id = new char[l];
		for (int i = 0; i < id.length; i++) {
			id[i] = (char) bytes.get();
		}
		return new Message(new String(id), timestamp, sensorType, value, idTs);
	}

}
