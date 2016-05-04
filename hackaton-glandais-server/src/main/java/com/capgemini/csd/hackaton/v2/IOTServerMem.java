package com.capgemini.csd.hackaton.v2;

import java.util.Map;

import com.capgemini.csd.hackaton.Util;
import com.capgemini.csd.hackaton.v2.mem.Mem;
import com.capgemini.csd.hackaton.v2.mem.MemBasic;

import io.airlift.airline.Command;
import net.openhft.chronicle.queue.ExcerptTailer;

@Command(name = "server-mem", description = "Serveur Mem")
public class IOTServerMem extends AbstractIOTServer {

	@Override
	protected void init() {
		// noop
	}

	@Override
	protected Mem getMem() {
		MemBasic memStore = new MemBasic(true);
		ExcerptTailer tailer = queueToPersist.createTailer();
		String json = tailer.readText();
		while (json != null) {
			Map message = Util.fromJson(json);
			String id = (String) message.get("id");
			memStore.putId(id);
			memStore.index(message);
			memStore.clean();
			json = tailer.readText();
		}
		return memStore;
	}

	@Override
	protected boolean containsId(String id) {
		return false;
	}

}
