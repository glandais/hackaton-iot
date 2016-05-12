package com.capgemini.csd.hackaton.v2.queue.impl;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * In order to surpass memory map limits, this class creates multiples
 * {@link MemoryMappedFIFOQueue} inside a folder. Given that reading and writing
 * is sequential, only a maximum of two file are open at time, one for writing
 * and one for reading.
 * 
 * @param <T>
 *            Type of objects to be inserted
 */
public class MultiFileMMapFifoQueue<T> implements EventQueue<T> {

	private List<FIFOByteArrayQueue> queues;

	private final int eachFileSize;
	private final File queueFolder;
	private final Serializer<T> s;

	private int readQueueNum;
	private int writeQueueNum;

	private int size;

	public MultiFileMMapFifoQueue(File queueFolder, Serializer<T> s, int eachFileSize) throws IOException {
		this.queueFolder = queueFolder;
		this.s = s;
		this.eachFileSize = eachFileSize;
		this.queues = new ArrayList<FIFOByteArrayQueue>();
		this.size = 0;

		this.writeQueueNum = 0;
		this.readQueueNum = 0;
		//Creating first queue!!!
		updateWriteReference();
	}

	private void updateWriteReference() {
		try {
			if (queues.size() == 0 || getWriteQueue().remaining() < FIFOByteArrayQueue.MAX_ENTRY_SIZE) {
				if (readQueueNum != writeQueueNum) { //Closing queue if not still being read
					getWriteQueue().shutdownAndSync();
				}

				File f = new File(queueFolder.getAbsolutePath() + File.separator + queues.size());

				FIFOByteArrayQueue q = new FIFOByteArrayQueue(f, eachFileSize);
				q.createAndOpen();

				queues.add(q);
				writeQueueNum = queues.size() - 1;
			}
		} catch (IOException e) {
			throw new IllegalStateException("Unable to shutdown queue");
		}
	}

	private void updateReadReference() {
		if (getReadQueue().size() == 0 && getReadQueue().remaining() < FIFOByteArrayQueue.MAX_ENTRY_SIZE) {
			try {
				if (getReadQueue().isOpen()) { //Maybe a put operation already closed the queue
					getReadQueue().shutdownAndSync(); //closing queue!
				}

				readQueueNum++;
			} catch (IOException e) {
				throw new IllegalStateException("Unable to shutdown queue");
			}
		}

		if (!getReadQueue().isOpen()) { //If queue not open, then open.
			try {
				getReadQueue().reopen();
			} catch (IOException e) {
				throw new IllegalStateException("Unable to open queue");
			}
		}
	}

	private FIFOByteArrayQueue getWriteQueue() {
		return queues.get(writeQueueNum);
	}

	private FIFOByteArrayQueue getReadQueue() {
		return queues.get(readQueueNum);
	}

	@Override
	public void put(T t) {
		byte[] checkpointData = s.toByteArray(t);
		updateWriteReference();
		getWriteQueue().put(checkpointData);
		size++;
	}

	@Override
	public T take() {
		if (size == 0) {
			throw new IllegalStateException("Queue empty!");
		}

		updateReadReference();
		size--;
		return s.interpret(getReadQueue().take());
	}

	@Override
	public int size() {
		return size;
	}

	public void shutdownAndSyncAll() throws IOException {
		for (FIFOByteArrayQueue mmap : queues) {
			if (mmap.isOpen()) {
				mmap.shutdownAndSync();
			}
		}
	}

	public void shutdownAndDeleteAll() throws IOException {
		for (FIFOByteArrayQueue mmap : queues) {
			if (mmap.isOpen()) {
				mmap.shutdownAndDelete();
			} else {
				mmap.deleteFileOnly();
			}
		}
	}
}