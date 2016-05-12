package com.capgemini.csd.hackaton.v2.queue;

import com.capgemini.csd.hackaton.v2.message.Message;

public interface Queue {

	Message readMessage();

	void put(Message mes);

	void close();

}
