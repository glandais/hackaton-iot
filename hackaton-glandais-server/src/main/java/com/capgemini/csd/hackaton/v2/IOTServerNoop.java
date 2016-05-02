package com.capgemini.csd.hackaton.v2;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.capgemini.csd.hackaton.v2.mem.Mem;
import com.capgemini.csd.hackaton.v2.synthese.Summary;

import io.airlift.airline.Command;

@Command(name = "server-noop", description = "Serveur Noop")
public class IOTServerNoop extends AbstractIOTServer {

	@Override
	protected void init() {
		// noop
	}

	protected void process(String json) {
		// mise en queue pour la persistence
		//		queueToPersist.createAppender().writeText(json);
	}

	@Override
	protected boolean containsId(String id) {
		return false;
	}

	@Override
	protected Mem getMem() {
		return new Mem() {

			@Override
			public Map<Integer, Summary> getSummary(long timestamp, Integer duration) {
				return Collections.emptyMap();
			}

			@Override
			public boolean containsId(String id) {
				return false;
			}

			@Override
			public void removeMessages(List<Map<String, Object>> messages) {
			}

			@Override
			public void putId(String id) {
			}

			@Override
			public void index(Map<String, Object> message) {
			}

			@Override
			public long getSize() {
				return 0;
			}

			@Override
			public long getMemorySize() {
				return 0;
			}
		};
	}

}
