package com.capgemini.csd.hackaton.v3.summaries;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.openhft.koloboke.collect.map.hash.HashLongObjMaps;

public class AllSummaries {

	public final static Logger LOGGER = LoggerFactory.getLogger(AllSummaries.class);

	protected Map<Long, Summaries> allSummaries = HashLongObjMaps.newMutableMap();

	//	private DB db;

	//	private HTreeMap<Long, Summaries> map;

	public void init(String dossier) {
		//		db = DBMaker.fileDB(new File(dossier, "summaries")).fileMmapEnable().concurrencyDisable().checksumHeaderBypass()
		//				.make();
		//		map = db.hashMap("summaries", new SerializerLong(), new SummariesSerializer()).createOrOpen();
		//		Set<Entry<Long, Summaries>> entrySet = map.entrySet();
		//		for (Entry<Long, Summaries> entry : entrySet) {
		//			allSummaries.put(entry.getKey(), entry.getValue());
		//		}
	}

	public void close() {
		//		for (Entry<Long, Summaries> entry : allSummaries.entrySet()) {
		//			map.put(entry.getKey(), entry.getValue());
		//		}
		//		db.close();
	}

	public synchronized Summaries get(long secondes) {
		Summaries summaries = allSummaries.get(secondes);
		if (summaries == null) {
			summaries = new Summaries();
			allSummaries.put(secondes, summaries);
		}
		return summaries;
	}

}
