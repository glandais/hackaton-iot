package com.capgemini.csd.hackaton.v3.summaries;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

import org.mapdb.elsa.ElsaMaker;
import org.mapdb.elsa.SerializerPojo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.capgemini.csd.hackaton.v3.IOTServerV3;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;

public class AllSummaries extends CacheLoader<Long, Summaries> implements RemovalListener<Long, Summaries> {

	public final static Logger LOGGER = LoggerFactory.getLogger(AllSummaries.class);

	protected LoadingCache<Long, Summaries> summaries = CacheBuilder.newBuilder().maximumSize(86400)
			.removalListener(this).build(this);

	private String dossier;

	public void init(String dossier) {
		this.dossier = dossier;
	}

	private File getFile(Long sec) {
		return new File(dossier, sec + ".summary");
	}

	@Override
	public void onRemoval(RemovalNotification<Long, Summaries> notification) {
		SerializerPojo serializer = new ElsaMaker().make();
		try {
			RandomAccessFile fos = new RandomAccessFile(getFile(notification.getKey()), "rw");
			serializer.serialize(fos, notification.getValue());
			fos.close();
		} catch (IOException e) {
			LOGGER.error("Erreur", e);
		}
	}

	@Override
	public Summaries load(Long key) throws Exception {
		File file = getFile(key);
		if (file.exists()) {
			SerializerPojo serializer = new ElsaMaker().make();
			RandomAccessFile fos = null;
			try {
				fos = new RandomAccessFile(file, "r");
				return (Summaries) serializer.deserialize(fos, -1);
			} catch (IOException e) {
				return new Summaries();
			} finally {
				if (fos != null) {
					fos.close();
				}
			}
		} else {
			return new Summaries();
		}
	}

	public Summaries get(long secondes) {
		return summaries.getUnchecked(secondes);
	}

}
