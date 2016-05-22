package com.capgemini.csd.hackaton.v3;

import com.capgemini.csd.hackaton.v3.messages.AllMessages;
import com.capgemini.csd.hackaton.v3.messages.Message;
import com.capgemini.csd.hackaton.v3.summaries.AllSummaries;
import com.capgemini.csd.hackaton.v3.summaries.Summaries;

public class Store {

	private AllSummaries allSummaries = new AllSummaries();

	private AllMessages allMessages;

	public Store() {
		super();
		this.allMessages = new AllMessages();
	}

	public void init(String dossier) {
		allMessages.init(dossier);
	}

	public void process(Message message) {
		long sec = message.getTimestamp() / 1000;
		allSummaries.get(sec).get(message.getSensorType()).accept(message.getValue());
		allMessages.getForWrite(sec).add(message);
	}

	public Summaries getSynthese(long from, long to) {
		long s = (from / 1000) + 1;
		long e = (to / 1000);
		Summaries summaries = new Summaries();
		for (long i = s; i < e; i++) {
			summaries.combine(allSummaries.get(i));
		}
		summaries.combine(allMessages.getForRead(s - 1).getSummaries(from, to));
		summaries.combine(allMessages.getForRead(e).getSummaries(from, to));
		return summaries;
	}

}
