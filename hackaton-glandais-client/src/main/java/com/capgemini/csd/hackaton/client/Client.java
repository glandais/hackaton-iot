package com.capgemini.csd.hackaton.client;

public interface Client {

	void setHostPort(String host, int port);

	void sendMessages(int count, boolean randomTime);

	void sendMessage(String message);

	void sendMessage(boolean randomTime);

	String getSynthese();

	void shutdown();

}
