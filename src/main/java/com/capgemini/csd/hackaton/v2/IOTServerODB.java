package com.capgemini.csd.hackaton.v2;

import com.capgemini.csd.hackaton.v2.mem.Mem;
import com.capgemini.csd.hackaton.v2.mem.MemBasic;
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
		StoreObjectDB store = new StoreObjectDB();
		store.init(dossier);
		return store;
	}

}
