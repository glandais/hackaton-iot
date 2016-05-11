package com.capgemini.csd.hackaton.v2.store;

import java.util.List;

import com.capgemini.csd.hackaton.v2.SummaryComputer;
import com.capgemini.csd.hackaton.v2.message.Message;

public interface Store extends SummaryComputer {

	boolean containsId(String id);

	void indexMessages(List<Message> messages);

}
