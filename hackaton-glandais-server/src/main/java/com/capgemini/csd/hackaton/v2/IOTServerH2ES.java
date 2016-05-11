package com.capgemini.csd.hackaton.v2;

import com.capgemini.csd.hackaton.v2.mem.Mem;
import com.capgemini.csd.hackaton.v2.mem.MemBasic;
import com.capgemini.csd.hackaton.v2.store.ChainedStore;
import com.capgemini.csd.hackaton.v2.store.Store;
import com.capgemini.csd.hackaton.v2.store.StoreElasticSearch;
import com.capgemini.csd.hackaton.v2.store.StoreH2;

import io.airlift.airline.Command;

@Command(name = "server-h2es", description = "Serveur H2 et ES")
public class IOTServerH2ES extends AbstractIOTServer {

	@Override
	protected Mem getMem() {
		return new MemBasic();
	}

	@Override
	protected Store getStore() {
		StoreH2 storeH2 = new StoreH2();
		storeH2.init(dossier);

		StoreElasticSearch storeElasticSearch = new StoreElasticSearch();
		storeElasticSearch.init(dossier);

		return new ChainedStore(storeH2, storeElasticSearch);
	}

}
