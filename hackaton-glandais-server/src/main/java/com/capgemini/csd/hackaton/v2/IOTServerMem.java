package com.capgemini.csd.hackaton.v2;

import java.util.Map;

import com.capgemini.csd.hackaton.Util;
import com.capgemini.csd.hackaton.v2.mem.Mem;
import com.capgemini.csd.hackaton.v2.mem.MemBasic;
import com.capgemini.csd.hackaton.v2.store.Store;
import com.capgemini.csd.hackaton.v2.store.StoreNoop;

import io.airlift.airline.Command;
import net.openhft.chronicle.queue.ExcerptTailer;

@Command(name = "server-mem", description = "Serveur Mem")
public class IOTServerMem extends AbstractIOTServer {

	@Override
	protected Mem getMem() {
		MemBasic memStore = new MemBasic();
		ExcerptTailer tailer = queueToPersist.createTailer();
		String json = tailer.readText();
		while (json != null) {
			Map message = Util.fromJson(json);
			String id = (String) message.get("id");
			memStore.putId(id);
			memStore.index(message);
			json = tailer.readText();
		}
		return memStore;
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
