package com.capgemini.csd.hackaton.v1.queue;

import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.IndexTreeList;
import org.mapdb.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Deprecated
public class QueueMapDB implements Queue {

	public final static Logger LOGGER = LoggerFactory.getLogger(QueueMapDB.class);

	private DB db;

	private IndexTreeList<String> queue;

	@Override
	public void init(String dossier) {
		LOGGER.info("Initialisation du MapDB.");
		db = DBMaker.memoryDB().make();
		queue = db.indexTreeList("queue", Serializer.STRING).create();
	}

	@Override
	public void push(String message) {
		queue.add(message);
	}

	@Override
	public String getMessage() {
		if (queue.isEmpty()) {
			return null;
		} else {
			return queue.removeAt(0);
		}
	}

	@Override
	public void close() {
		db.close();
	}

}
