package com.capgemini.csd.hackaton.v2;

import com.capgemini.csd.hackaton.v2.mem.Mem;
import com.capgemini.csd.hackaton.v2.mem.MemBasic;
import com.capgemini.csd.hackaton.v2.store.Store;
import com.capgemini.csd.hackaton.v2.store.StoreElasticSearch;

import io.airlift.airline.Command;

@Command(name = "server-es", description = "Serveur ElasticSearch")
public class IOTServerES extends AbstractIOTStoreServer {

	@Override
	protected Mem getMem() {
		return new MemBasic(false);
	}

	@Override
	protected Store getStore() {
		StoreElasticSearch storeElasticSearch = new StoreElasticSearch();
		storeElasticSearch.init(dossier);
		return storeElasticSearch;
	}

}
