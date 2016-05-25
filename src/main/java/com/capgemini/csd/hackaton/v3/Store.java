package com.capgemini.csd.hackaton.v3;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.capgemini.csd.hackaton.v3.messages.AllMessagesGuava;
import com.capgemini.csd.hackaton.v3.messages.IAllMessages;
import com.capgemini.csd.hackaton.v3.messages.Message;
import com.capgemini.csd.hackaton.v3.summaries.AllSummaries;
import com.capgemini.csd.hackaton.v3.summaries.Summaries;

public class Store {

	public final static Logger LOGGER = LoggerFactory.getLogger(Store.class);

	private AllSummaries allSummaries = new AllSummaries();

	private IAllMessages allMessages = new AllMessagesGuava();

	public Store() {
		super();
	}

	public void init(String dossier) {
		allMessages.init(dossier);
		allSummaries.init(dossier);
	}

	public void close() {
		allMessages.close();
		allSummaries.close();
	}

	public void process(Message message) {
		long secondes = message.getSecondes();
		allMessages.getForWrite(secondes).add(message);
		allSummaries.get(secondes).get(message.getSensorType()).accept(message.getValue());
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
