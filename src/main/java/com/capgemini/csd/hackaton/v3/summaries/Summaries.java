package com.capgemini.csd.hackaton.v3.summaries;

import java.io.Serializable;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

public class Summaries implements Serializable {

	private static final long serialVersionUID = 4688043972119008161L;

	private TreeMap<Integer, Summary> summaries = new TreeMap<>();

	public Summaries() {
		super();
	}

	public Summaries(Map<Integer, Summary> map) {
		super();
		summaries.putAll(map);
	}

	public synchronized Summary get(int key) {
		Summary summary = summaries.get(key);
		if (summary == null) {
			summary = new Summary((int) key);
		}
		summaries.put((Integer) key, summary);
		return summary;
	}

	/**
	 * Appelé uniquement depuis Store.getSynthese, pas besoin de sync
	 * 
	 * @param summariesToCombine
	 */
	public void combine(Summaries summariesToCombine) {
		for (Map.Entry<Integer, Summary> entry : summariesToCombine.summaries.entrySet()) {
			Integer key = entry.getKey();
			Summary value = entry.getValue();
			if (summaries.containsKey(key)) {
				summaries.get(key).combine(value);
			} else {
				summaries.put(key, new Summary(value));
			}
		}
	}

	/**
	 * Appelé uniquement depuis Store.getSynthese, pas besoin de sync
	 * 
	 */
	@Override
	public String toString() {
		return "[" + summaries.values().stream().map(s -> s.toString()).collect(Collectors.joining(",")) + "]";
	}

}
