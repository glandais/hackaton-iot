package com.capgemini.csd.hackaton.v2;

import com.capgemini.csd.hackaton.v2.mem.Mem;
import com.capgemini.csd.hackaton.v2.mem.MemBasic;
import com.capgemini.csd.hackaton.v2.store.Store;
import com.capgemini.csd.hackaton.v2.store.StoreObjectDB;

public class IOTServerODB extends AbstractIOTStoreServer {

	@Override
	protected Mem getMem() {
		return new MemBasic(false);
	}

	@Override
	protected Store getStore() {
		StoreObjectDB store = new StoreObjectDB();
		store.init(dossier);
		return store;
	}

}
