package com.capgemini.csd.hackaton.v2;

import com.capgemini.csd.hackaton.v2.mem.Mem;
import com.capgemini.csd.hackaton.v2.mem.MemBasic;
import com.capgemini.csd.hackaton.v2.queue.Queue;
import com.capgemini.csd.hackaton.v2.queue.QueueMem;
import com.capgemini.csd.hackaton.v2.store.Store;
import com.capgemini.csd.hackaton.v2.store.StoreLucene;

import io.airlift.airline.Command;

@Command(name = "server-lucene", description = "Serveur Lucene")
public class IOTServerLucene extends AbstractIOTServer {

	@Override
	protected Queue getQueue() {
		return new QueueMem();
	}

	@Override
	protected Mem getMem() {
		return new MemBasic();
	}

	@Override
	protected Store getStore() {
		return new StoreLucene(512);
	}

}
