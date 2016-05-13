package com.capgemini.csd.hackaton.v2.store.lucene;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorer;

import com.capgemini.csd.hackaton.client.Summary;

import net.openhft.koloboke.collect.map.hash.HashIntObjMaps;

public class SummaryCollector implements Collector, LeafCollector {

	private DirectoryReader ireader;

	private Map<Integer, Summary> summaries;

	private static final Set<String> FIELDS = new HashSet<>(Arrays.asList("sensorType", "value"));

	public SummaryCollector(DirectoryReader ireader) {
		super();
		this.ireader = ireader;
		this.summaries = HashIntObjMaps.newMutableMap();
	}

	@Override
	public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
		return this;
	}

	@Override
	public boolean needsScores() {
		return false;
	}

	@Override
	public void setScorer(Scorer scorer) throws IOException {
		// noop
	}

	@Override
	public void collect(int doc) throws IOException {
		Document document = ireader.document(doc, FIELDS);
		int sensorType = document.getField("sensorType").numericValue().intValue();
		long value = document.getField("value").numericValue().longValue();
		Summary summary = summaries.get(sensorType);
		if (summary == null) {
			summary = new Summary(sensorType);
			summaries.put(sensorType, summary);
		}
		summary.accept(value);
	}

	public Map<Integer, Summary> getSummaries() {
		return summaries;
	}

}
