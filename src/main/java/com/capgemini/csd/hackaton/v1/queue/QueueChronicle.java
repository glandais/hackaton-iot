package com.capgemini.csd.hackaton.v1.queue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ChronicleQueueBuilder;
import net.openhft.chronicle.queue.ExcerptTailer;

public class QueueChronicle implements Queue {

	public final static Logger LOGGER = LoggerFactory.getLogger(QueueChronicle.class);

	private ChronicleQueue chronicle;

	private ExcerptTailer tailer;

	public QueueChronicle() {
		super();
	}

	@Override
	public void init(String dossier) {
		LOGGER.info("Initialisation du chronicle.");
		chronicle = ChronicleQueueBuilder.single(dossier).build();
		tailer = chronicle.createTailer();
	}

	@Override
	public void push(String message) {
		chronicle.createAppender().writeText(message);
	}

	@Override
	public String getMessage() {
		return tailer.readText();
	}

	@Override
	public void close() {
		chronicle.close();
	}

}
