package com.capgemini.csd.hackaton.v2;

import com.capgemini.csd.hackaton.v2.mem.Mem;
import com.capgemini.csd.hackaton.v2.mem.MemMapDB;
import com.capgemini.csd.hackaton.v2.queue.Queue;
import com.capgemini.csd.hackaton.v2.queue.QueueMem;
import com.capgemini.csd.hackaton.v2.store.Store;
import com.capgemini.csd.hackaton.v2.store.StoreMapDB;

import io.airlift.airline.Command;

@Command(name = "server-mapdb2", description = "Serveur MapDB")
public class IOTServerMapDB2 extends AbstractIOTServer {

	@Override
	protected Mem getMem() {
		return new MemMapDB();
	}

	@Override
	protected Store getStore() {
		return new StoreMapDB();
	}

	@Override
	protected Queue getQueue() {
		return new QueueMem();
	}

}
