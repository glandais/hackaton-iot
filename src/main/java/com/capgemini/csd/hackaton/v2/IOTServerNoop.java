package com.capgemini.csd.hackaton.v2;

import com.capgemini.csd.hackaton.v2.mem.Mem;
import com.capgemini.csd.hackaton.v2.mem.MemNoop;
import com.capgemini.csd.hackaton.v2.store.Store;
import com.capgemini.csd.hackaton.v2.store.StoreNoop;

import io.airlift.airline.Command;

@Command(name = "server-noop", description = "Serveur Noop")
public class IOTServerNoop extends AbstractIOTServer {

	protected void process(String json) {
		// mise en queue pour la persistence
		//		queueToPersist.createAppender().writeText(json);
	}

	@Override
	protected boolean containsId(String id) {
		return false;
	}

	@Override
	protected Mem getMem() {
		return new MemNoop();
	}

	@Override
	protected Store getStore() {
		return new StoreNoop();
	}

}
