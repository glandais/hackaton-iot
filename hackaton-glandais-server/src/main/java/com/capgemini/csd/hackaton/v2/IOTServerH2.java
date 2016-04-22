package com.capgemini.csd.hackaton.v2;

import com.capgemini.csd.hackaton.v2.mem.Mem;
import com.capgemini.csd.hackaton.v2.mem.MemBasic;
import com.capgemini.csd.hackaton.v2.store.Store;
import com.capgemini.csd.hackaton.v2.store.StoreH2;

import io.airlift.airline.Command;

@Command(name = "server-h2", description = "Serveur H2")
public class IOTServerH2 extends AbstractIOTStoreServer {

	@Override
	protected Mem getMem() {
		return new MemBasic(false);
	}

	@Override
	protected Store getStore() {
		StoreH2 storeH2 = new StoreH2();
		storeH2.init(dossier);
		return storeH2;
	}

}
