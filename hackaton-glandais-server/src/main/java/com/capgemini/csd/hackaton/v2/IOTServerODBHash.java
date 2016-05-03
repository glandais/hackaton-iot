package com.capgemini.csd.hackaton.v2;

import com.capgemini.csd.hackaton.v2.mem.Mem;
import com.capgemini.csd.hackaton.v2.mem.MemBasic;
import com.capgemini.csd.hackaton.v2.store.Store;
import com.capgemini.csd.hackaton.v2.store.StoreObjectDBHash;

import io.airlift.airline.Command;

@Command(name = "server-odb-hash", description = "Serveur ObjectDB Hash")
public class IOTServerODBHash extends AbstractIOTStoreServer {

	@Override
	protected Mem getMem() {
		return new MemBasic(false);
	}

	@Override
	protected Store getStore() {
		StoreObjectDBHash store = new StoreObjectDBHash();
		store.init(dossier);
		return store;
	}

}
