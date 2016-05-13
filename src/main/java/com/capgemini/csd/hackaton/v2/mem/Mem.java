package com.capgemini.csd.hackaton.v2.mem;

import java.util.List;
import java.util.Map;

import com.capgemini.csd.hackaton.v2.SummaryComputer;
import com.capgemini.csd.hackaton.v2.message.Message;
import com.capgemini.csd.hackaton.v2.message.Timestamp;

public interface Mem extends SummaryComputer {

	boolean containsId(String id);

	void index(Message message);

	void removeMessages(List<Message> messages);

	void putId(String id);

	long getSize();

	void close();

}
