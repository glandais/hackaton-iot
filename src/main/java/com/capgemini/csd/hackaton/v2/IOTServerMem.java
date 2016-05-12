package com.capgemini.csd.hackaton.v2;

import com.capgemini.csd.hackaton.v2.mem.Mem;
import com.capgemini.csd.hackaton.v2.mem.MemBasic;
import com.capgemini.csd.hackaton.v2.store.Store;
import com.capgemini.csd.hackaton.v2.store.StoreNoop;

import io.airlift.airline.Command;

@Command(name = "server-mem", description = "Serveur Mem")
public class IOTServerMem extends AbstractIOTServer {

	@Override
	protected Mem getMem() {
		return new MemBasic();
	}

	@Override
	protected boolean containsId(String id) {
		return false;
	}

	@Override
	protected Store getStore() {
		return new StoreNoop();
	}

}
