package com.capgemini.csd.hackaton.v2;

import java.math.BigDecimal;

import org.boon.json.serializers.CustomObjectSerializer;
import org.boon.json.serializers.JsonSerializerInternal;
import org.boon.primitive.CharBuf;

public class BigDecimalSerializer implements CustomObjectSerializer<BigDecimal> {

	@Override
	public Class<BigDecimal> type() {
		return BigDecimal.class;
	}

	@Override
	public void serializeObject(JsonSerializerInternal serializer, BigDecimal instance, CharBuf builder) {
		builder.add(instance.toString());
	}

}
