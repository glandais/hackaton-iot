package com.capgemini.csd.hackaton.v2;

import com.capgemini.csd.hackaton.v2.mem.Mem;
import com.capgemini.csd.hackaton.v2.mem.MemBasic;
import com.capgemini.csd.hackaton.v2.store.Store;
import com.capgemini.csd.hackaton.v2.store.StoreMapDB;

import io.airlift.airline.Command;

@Command(name = "server-mapdb", description = "Serveur MapDB")
public class IOTServerMapDB extends AbstractIOTStoreServer {

	@Override
	protected Mem getMem() {
		return new MemBasic(false);
	}

	@Override
	protected Store getStore() {
		StoreMapDB storeMapDB = new StoreMapDB();
		storeMapDB.init(dossier);
		return storeMapDB;
	}

}
