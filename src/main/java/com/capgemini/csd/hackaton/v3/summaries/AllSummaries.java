package com.capgemini.csd.hackaton.v3.summaries;

import java.util.concurrent.ConcurrentHashMap;

public class AllSummaries extends ConcurrentHashMap<Long, Summaries> {

	private static final long serialVersionUID = 3352093342867165952L;

	@Override
	public Summaries get(Object key) {
		Summaries summaries = super.get(key);
		if (summaries == null) {
			summaries = new Summaries();
		}
		super.put((Long) key, summaries);
		return summaries;
	}
}
