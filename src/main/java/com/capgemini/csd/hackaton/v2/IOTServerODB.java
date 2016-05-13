package com.capgemini.csd.hackaton.v2;

import com.capgemini.csd.hackaton.v2.mem.Mem;
import com.capgemini.csd.hackaton.v2.mem.MemBasic;
import com.capgemini.csd.hackaton.v2.queue.Queue;
import com.capgemini.csd.hackaton.v2.queue.QueueMem;
import com.capgemini.csd.hackaton.v2.store.Store;
import com.capgemini.csd.hackaton.v2.store.StoreObjectDB;

import io.airlift.airline.Command;

@Command(name = "server-odb", description = "Serveur ObjectDB")
public class IOTServerODB extends AbstractIOTServer {

	@Override
	protected Mem getMem() {
		return new MemBasic();
	}

	@Override
	protected Store getStore() {
		return new StoreObjectDB();
	}

	@Override
	protected Queue getQueue() {
		return new QueueMem();
	}

}
