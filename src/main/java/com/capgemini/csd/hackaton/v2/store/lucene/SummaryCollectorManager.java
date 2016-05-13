package com.capgemini.csd.hackaton.v2.store.lucene;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.search.CollectorManager;

import com.capgemini.csd.hackaton.client.Summary;

public class SummaryCollectorManager implements CollectorManager<SummaryCollector, Map<Integer, Summary>> {

	private DirectoryReader ireader;

	public SummaryCollectorManager(DirectoryReader ireader) {
		super();
		this.ireader = ireader;
	}

	@Override
	public SummaryCollector newCollector() throws IOException {
		return new SummaryCollector(ireader);
	}

	@Override
	public Map<Integer, Summary> reduce(Collection<SummaryCollector> collectors) throws IOException {
		Map<Integer, Summary> result = new HashMap<>();
		for (SummaryCollector summaryCollector : collectors) {
			Map<Integer, Summary> summaries = summaryCollector.getSummaries();
			for (Entry<Integer, Summary> summary : summaries.entrySet()) {
				if (result.get(summary.getKey()) == null) {
					result.put(summary.getKey(), summary.getValue());
				} else {
					result.get(summary.getKey()).combine(summary.getValue());
				}
			}
		}
		return result;
	}

}
