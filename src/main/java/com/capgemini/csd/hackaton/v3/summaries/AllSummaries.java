package com.capgemini.csd.hackaton.v3.summaries;

import java.io.File;

import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.serializer.SerializerLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;

public class AllSummaries extends CacheLoader<Long, Summaries> implements RemovalListener<Long, Summaries> {

	public final static Logger LOGGER = LoggerFactory.getLogger(AllSummaries.class);

	private DB db;

	protected LoadingCache<Long, Summaries> summaries = CacheBuilder.newBuilder().maximumSize(86400)
			.removalListener(this).build(this);

	private HTreeMap<Long, Summaries> map;

	public void init(String dossier) {
		db = DBMaker.fileDB(new File(dossier, "summaries")).fileMmapEnable().concurrencyDisable().checksumHeaderBypass()
				.make();
		map = db.hashMap("summaries", new SerializerLong(), new SummariesSerializer()).createOrOpen();
	}

	public void close() {
		db.close();
	}

	@Override
	public void onRemoval(RemovalNotification<Long, Summaries> notification) {
		map.put(notification.getKey(), notification.getValue());
	}

	@Override
	public Summaries load(Long key) throws Exception {
		if (map.containsKey(key)) {
			return map.get(key);
		} else {
			return new Summaries();
		}
	}

	public Summaries get(long secondes) {
		return summaries.getUnchecked(secondes);
	}

}
