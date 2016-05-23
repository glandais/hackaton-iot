package com.capgemini.csd.hackaton.v3.summaries;

import java.io.IOException;

import org.mapdb.DataInput2;
import org.mapdb.DataOutput2;
import org.mapdb.elsa.ElsaMaker;
import org.mapdb.elsa.SerializerPojo;
import org.mapdb.serializer.GroupSerializerObjectArray;

final class SummariesSerializer extends GroupSerializerObjectArray<Summaries> {

	protected SerializerPojo serializer = new ElsaMaker().registerClasses(Summaries.class, Summary.class).make();

	@Override
	public void serialize(DataOutput2 out, Summaries value) throws IOException {
		serializer.serialize(out, value);
	}

	@Override
	public Summaries deserialize(DataInput2 input, int available) throws IOException {
		return (Summaries) serializer.deserialize(input, available);
	}
}