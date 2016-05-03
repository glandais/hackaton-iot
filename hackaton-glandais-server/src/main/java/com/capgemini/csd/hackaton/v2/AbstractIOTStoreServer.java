package com.capgemini.csd.hackaton.v2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicBoolean;

import org.boon.core.Sys;
import org.boon.json.JsonFactory;
import org.mapdb.Atomic;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.capgemini.csd.hackaton.v2.mem.Mem;
import com.capgemini.csd.hackaton.v2.store.Store;
import com.capgemini.csd.hackaton.v2.synthese.Summary;

import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.TailerDirection;

public abstract class AbstractIOTStoreServer extends AbstractIOTServer {

	private static final long SLEEP_PERSISTER = 5000L;

	private static final int CACHE_SIZE_REAL = 1100000;

	private static int CACHE_SIZE = 1100000;

	private static final int BATCH_SIZE = 1000;

	public final static Logger LOGGER = LoggerFactory.getLogger(AbstractIOTStoreServer.class);

	protected Atomic.String lastInsertedStore;

	protected AtomicBoolean doIndex = new AtomicBoolean(false);

	protected Store store;

	@Override
	public void init() {
		store = getStore();

		DB db = DBMaker.fileDB(dossier + "/lastIndexed").checksumHeaderBypass().make();
		lastInsertedStore = db.atomicString("lastIndexed").createOrOpen();
		// récupération de l'id du dernier message enregistré
		String lastPersistedId = getLastInsertedId();

		// enregistrement des messages non enregistrés
		ExcerptTailer tailerToPersist = queueToPersist.createTailer().direction(TailerDirection.BACKWARD).toEnd();
		index(tailerToPersist, lastPersistedId, true);

		// FIXME RAZ queueToPersist quand tout a été persisté

		startPersister();
	}

	protected abstract Mem getMem();

	protected abstract Store getStore();

	protected void awaitWarmupTermination() {
		while (mem.getSize() != 0) {
			try {
				Thread.sleep(10L);
			} catch (InterruptedException e) {
				LOGGER.error(":(", e);
			}
		}
		CACHE_SIZE = CACHE_SIZE_REAL;
	}

	protected int getWarmupMessageCount() {
		CACHE_SIZE = super.getWarmupMessageCount() - 100;
		return super.getWarmupMessageCount();
	}

	@Override
	protected boolean containsId(String id) {
		return store.containsId(id);
	}

	protected Map<Integer, Summary> getSummary(long timestamp, Integer duration) {
		Map<Integer, Summary> storeSummary = store.getSummary(timestamp, duration);
		Map<Integer, Summary> memSummary = super.getSummary(timestamp, duration);
		Map<Integer, Summary> summary = new HashMap<Integer, Summary>();

		for (Entry<Integer, Summary> entry : storeSummary.entrySet()) {
			if (memSummary.containsKey(entry.getKey())) {
				entry.getValue().combine(memSummary.get(entry.getKey()));
			}
			summary.put(entry.getKey(), entry.getValue());
		}
		for (Entry<Integer, Summary> entry : memSummary.entrySet()) {
			if (!storeSummary.containsKey(entry.getKey())) {
				summary.put(entry.getKey(), entry.getValue());
			}
		}
		return summary;
	}

	@Override
	protected String getSynthese(long timestamp, int duration) {
		String synthese = super.getSynthese(timestamp, duration);
		//		doIndex.set(true);
		return synthese;
	}

	@Override
	protected void index() {
		doIndex.set(true);
	}

	private void startPersister() {
		Thread thread = new Thread(() -> {
			ExcerptTailer tailerToPersist = queueToPersist.createTailer();
			tailerToPersist.toEnd();
			while (true) {
				if (mem.getSize() > CACHE_SIZE || doIndex.getAndSet(false)) {
					index(tailerToPersist, null, false);
					Sys.sleep(1L);
				} else {
					Sys.sleep(SLEEP_PERSISTER);
				}
			}
		});
		thread.setName("persister");
		thread.setPriority(Thread.MIN_PRIORITY);
		thread.start();
	}

	private void index(ExcerptTailer tailerToPersist, String lastIndexed, boolean initial) {
		List<Map<String, Object>> messages = new ArrayList<>(BATCH_SIZE + 1);

		String message = tailerToPersist.readText();
		String precedent = null;
		if (initial && message != null) {
			Map<String, Object> map = JsonFactory.fromJson(message, Map.class);
			setLastInsertedId((String) map.get("id"));
		}
		while (message != null) {
			Map<String, Object> map = JsonFactory.fromJson(message, Map.class);
			if (lastIndexed != null && lastIndexed.equals(map.get("id"))) {
				break;
			}
			messages.add(map);
			if (messages.size() == BATCH_SIZE) {
				indexMessages(messages, initial);
				messages = new ArrayList<>(BATCH_SIZE + 1);
			}
			message = tailerToPersist.readText();
			if (message != null && message.equals(precedent)) {
				break;
			}
			precedent = message;
		}
		if (messages.size() > 0) {
			indexMessages(messages, initial);
		}
	}

	private void indexMessages(List<Map<String, Object>> messages, boolean initial) {
		LOGGER.info("Indexing " + messages.size() + " messages");
		indexLock.writeLock().lock();
		try {
			store.indexMessages(messages);
			mem.removeMessages(messages);
			if (!initial) {
				String id = (String) messages.get(messages.size() - 1).get("id");
				setLastInsertedId(id);
			}
		} finally {
			indexLock.writeLock().unlock();
		}
	}

	private String getLastInsertedId() {
		return lastInsertedStore.get();
	}

	private void setLastInsertedId(String id) {
		lastInsertedStore.set(id);
	}

}
