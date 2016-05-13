package com.capgemini.csd.hackaton.v2;

import com.capgemini.csd.hackaton.v2.mem.Mem;
import com.capgemini.csd.hackaton.v2.mem.MemNoop;
import com.capgemini.csd.hackaton.v2.queue.Queue;
import com.capgemini.csd.hackaton.v2.queue.QueueMem;
import com.capgemini.csd.hackaton.v2.store.Store;
import com.capgemini.csd.hackaton.v2.store.StoreNoop;

import io.airlift.airline.Command;

@Command(name = "server-noop", description = "Serveur Noop")
public class IOTServerNoop extends AbstractIOTServer {

	protected void process(String json) {
		// mise en queue pour la persistence
	}

	@Override
	protected Mem getMem() {
		return new MemNoop();
	}

	@Override
	protected Store getStore() {
		return new StoreNoop();
	}

	@Override
	protected Queue getQueue() {
		return new QueueMem();
	}

}
