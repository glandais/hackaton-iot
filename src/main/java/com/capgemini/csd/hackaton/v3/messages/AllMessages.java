package com.capgemini.csd.hackaton.v3.messages;

import java.io.File;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.boon.collections.ConcurrentHashSet;
import org.boon.core.Sys;

import com.capgemini.csd.hackaton.v3.Messages;
import com.capgemini.csd.hackaton.v3.messages.mapdb.MessagesMapDB;
import com.capgemini.csd.hackaton.v3.messages.mem.MessagesMem;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;

public class AllMessages extends CacheLoader<Long, Messages> implements RemovalListener<Long, Messages> {

	private String dossier;

	protected Map<Long, Messages> reading = new ConcurrentHashMap<>();

	protected Map<Long, Messages> writing = new ConcurrentHashMap<>();

	protected LoadingCache<Long, Messages> messages = CacheBuilder.newBuilder().expireAfterWrite(10, TimeUnit.SECONDS)
			.removalListener(this).build(this);

	private Set<Long> writeLock = new ConcurrentHashSet<>();

	public void init(String dossier) {
		this.dossier = dossier;
	}

	private File getFile(Long sec) {
		return new File(dossier, sec + "");
	}

	@Override
	public Messages load(Long sec) throws Exception {
		Messages result;
		File file = getFile(sec);
		if (file.exists()) {
			result = new MessagesMapDB(file);
		} else {
			result = new MessagesMem();
		}
		reading.put(sec, result);
		writing.put(sec, result);
		return result;
	}

	@Override
	public void onRemoval(RemovalNotification<Long, Messages> notification) {
		Long sec = notification.getKey();
		if (notification.getValue() instanceof MessagesMem) {
			writeLock.add(sec);
			MessagesMapDB messagesMapDB = new MessagesMapDB(getFile(sec));
			messagesMapDB.putAll(((MessagesMem) notification.getValue()).getMap());
			reading.put(sec, messagesMapDB);
			writing.put(sec, messagesMapDB);
			writeLock.remove(sec);
		} else if (notification.getValue() instanceof MessagesMapDB) {
			reading.remove(sec);
			writing.remove(sec);
			((MessagesMapDB) notification.getValue()).close();
		}
	}

	public Messages getForWrite(long sec) {
		while (writeLock.contains(sec)) {
			Sys.sleep(1L);
		}
		Messages result = writing.get(sec);
		if (result != null) {
			return result;
		}
		return messages.getUnchecked(sec);
	}

	public Messages getForRead(long sec) {
		Messages result = reading.get(sec);
		if (result != null) {
			return result;
		}
		return messages.getUnchecked(sec);
	}

}
