package com.capgemini.csd.hackaton.v1.index;

public interface Index {

	void init(String dossier);

	boolean isInMemory();

	void index(String json) throws Exception;

	String getSynthese();

	void close();

	long getSize();

}
