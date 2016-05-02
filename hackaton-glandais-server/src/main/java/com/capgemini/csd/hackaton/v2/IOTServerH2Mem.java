package com.capgemini.csd.hackaton.v2;

import com.capgemini.csd.hackaton.v2.mem.Mem;
import com.capgemini.csd.hackaton.v2.mem.MemBasic;
import com.capgemini.csd.hackaton.v2.store.Store;
import com.capgemini.csd.hackaton.v2.store.StoreH2Mem;

import io.airlift.airline.Command;

@Command(name = "server-h2mem", description = "Serveur H2 avec ID en mémoire")
public class IOTServerH2Mem extends AbstractIOTStoreServer {

	@Override
	protected Mem getMem() {
		return new MemBasic(false);
	}

	@Override
	protected Store getStore() {
		StoreH2Mem storeH2 = new StoreH2Mem();
		storeH2.init(dossier);
		return storeH2;
	}

}
