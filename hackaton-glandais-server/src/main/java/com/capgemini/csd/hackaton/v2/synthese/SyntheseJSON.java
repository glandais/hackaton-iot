package com.capgemini.csd.hackaton.v2.synthese;

import java.math.BigDecimal;

import org.boon.json.JsonParserFactory;
import org.boon.json.JsonSerializerFactory;
import org.boon.json.ObjectMapper;
import org.boon.json.implementation.ObjectMapperImpl;

public class SyntheseJSON {

	public static final ObjectMapper getObjectMapper() {
		JsonParserFactory jsonParserFactory = new JsonParserFactory();
		jsonParserFactory.lax();
		JsonSerializerFactory serializerFactory = new JsonSerializerFactory();
		serializerFactory.addTypeSerializer(BigDecimal.class, new BigDecimalSerializer());
		ObjectMapperImpl om = new ObjectMapperImpl(jsonParserFactory, serializerFactory);
		return om;
	}

}
