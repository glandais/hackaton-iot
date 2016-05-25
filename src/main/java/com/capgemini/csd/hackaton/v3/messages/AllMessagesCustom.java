package com.capgemini.csd.hackaton.v3.messages;

import java.io.File;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.boon.collections.ConcurrentHashSet;
import org.boon.core.Sys;

import com.capgemini.csd.hackaton.beans.Timestamp;
import com.capgemini.csd.hackaton.beans.Value;
import com.capgemini.csd.hackaton.v3.Messages;
import com.capgemini.csd.hackaton.v3.messages.mapdb.MessagesMapDB;
import com.capgemini.csd.hackaton.v3.messages.mem.MessagesMem;

import it.unimi.dsi.fastutil.PriorityQueue;
import it.unimi.dsi.fastutil.longs.LongArrayPriorityQueue;
import it.unimi.dsi.fastutil.longs.LongPriorityQueues;
import net.openhft.koloboke.collect.map.hash.HashLongObjMaps;

public class AllMessagesCustom implements IAllMessages {

	private static final int MAX_DISK = 2;

	private static final int MAX_MEM = 60;

	protected ExecutorService executor = Executors.newSingleThreadExecutor();

	private Set<Long> copyLock = new ConcurrentHashSet<>();

	private PriorityQueue<Long> memSecs = LongPriorityQueues.synchronize(new LongArrayPriorityQueue(MAX_MEM * 2));

	private PriorityQueue<Long> diskSecs = LongPriorityQueues.synchronize(new LongArrayPriorityQueue(MAX_DISK * 2));

	private Map<Long, MessagesMem> mem = Collections.synchronizedMap(HashLongObjMaps.newMutableMap());

	private Map<Long, MessagesMapDB> disk = Collections.synchronizedMap(HashLongObjMaps.newMutableMap());

	private String dossier;

	public static void main(String[] args) {
		AllMessagesCustom allMessages = new AllMessagesCustom();
		allMessages.init("D:\\tmp");
		for (int i = 0; i < 70; i++) {
			allMessages.get(i, true);
		}
		Sys.sleep(5000L);
		for (int i = 0; i < 5; i++) {
			allMessages.get(i, true);
		}
		// allMessages.get(0, true);
		new Thread(() -> {
			allMessages.get(0, true);
		}).start();
		new Thread(() -> {
			allMessages.get(0, true);
		}).start();
	}

	public void init(String dossier) {
		this.dossier = dossier;
		new Thread(() -> {
			while (true) {
				cleanupDisk();
				cleanupMem();
				Sys.sleep(1000L);
			}
		}).start();
	}

	private File getFile(Long sec) {
		return new File(dossier, "" + sec);
	}

	public void close() {

	}

	public Messages getForWrite(long s) {
		return get(s, true);
	}

	public Messages getForRead(long s) {
		return get(s, false);
	}

	public Messages get(long s, boolean write) {
		if (write && copyLock.contains(s)) {
			while (copyLock.contains(s)) {
				Sys.sleep(1L);
			}
		}
		return getSync(s, write);
	}

	public synchronized Messages getSync(long s, boolean write) {
		Messages messages = mem.get(s);
		if (messages != null) {
			return messages;
		}
		messages = disk.get(s);
		if (messages != null) {
			return messages;
		}
		File file = getFile(s);
		if (file.exists()) {
			messages = new MessagesMapDB(file);
			disk.put(s, (MessagesMapDB) messages);
			diskSecs.enqueue(s);
			return messages;
		} else {
			messages = new MessagesMem();
			mem.put(s, (MessagesMem) messages);
			memSecs.enqueue(s);
			return messages;
		}
	}

	private void cleanupMem() {
		while (memSecs.size() > MAX_MEM) {
			Long s = memSecs.dequeue();
			MessagesMem messages = mem.get(s);
			copyLock.add(s);
			executor.submit(() -> {
				MessagesMapDB messagesMapDB = new MessagesMapDB(getFile(s));
				Iterable<Entry<Timestamp, Value>> values = messages.getValues();
				for (Entry<Timestamp, Value> entry : values) {
					messagesMapDB.put(entry.getKey(), entry.getValue());
				}
				messagesMapDB.close();
				mem.remove(s);
				copyLock.remove(s);
			});
		}
	}

	private void cleanupDisk() {
		while (diskSecs.size() > MAX_DISK) {
			Long s = diskSecs.dequeue();
			MessagesMapDB messages = disk.remove(s);
			messages.close();
		}
	}

}
