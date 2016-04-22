package com.capgemini.csd.hackaton.index;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Date;
import java.util.Map;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.boon.json.JsonFactory;

public class IndexLucene implements Index {

	//	private final ReentrantLock lock = new ReentrantLock();

	private Directory index;

	private IndexWriter indexWriter;

	private DirectoryReader indexReader;

	public IndexLucene() {
		super();
	}

	@Override
	public void init(String dossier) {
		try {
			Path path = Paths.get(dossier);
			index = FSDirectory.open(path);
			Analyzer analyzer = new KeywordAnalyzer();
			IndexWriterConfig config = new IndexWriterConfig(analyzer);
			indexWriter = new IndexWriter(index, config);
			indexWriter.commit();
			indexReader = DirectoryReader.open(index);
		} catch (IOException e) {
			throw new IllegalArgumentException("Impossible de créer l'index", e);
		}
	}

	@Override
	public boolean isInMemory() {
		return false;
	}

	@Override
	public void index(String json) {
		try {
			indexE(json);
		} catch (IOException e) {
			throw new RuntimeException("?", e);
		}
	}

	public void indexE(String json) throws IOException {
		Map<?, ?> message = JsonFactory.create().fromJson(json, Map.class);
		String id = (String) message.get("id");
		Term idTerm = new Term("id", id);
		if (indexReader.docFreq(idTerm) > 0) {
			throw new RuntimeException("ID existant : " + id);
		}
		long timestamp = ((Date) message.get("timestamp")).getTime();
		int sensorId = ((Number) message.get("sensorType")).intValue();
		long value = ((Number) message.get("value")).longValue();
		Document doc = new Document();
		doc.add(new LongPoint("timestamp", timestamp));
		doc.add(new IntPoint("sensorType", sensorId));
		doc.add(new LongPoint("value", value));
		indexWriter.updateDocument(idTerm, doc);
		//		indexWriter.commit();
	}

	@Override
	public String getSynthese() {
		return "";
		//		long time = System.currentTimeMillis();
		//		long lo = time - 3600 * 1000;
		//		long hi = time;
		//		UUID from = new UUID(lo, 0);
		//		UUID to = new UUID(hi, 0);
		//		Stream<UUID> stream = map.subMap(from, to).values().stream();
		//
		//		Map<Long, LongSummaryStatistics> statsMap;
		//		synchronized (map) {
		//			statsMap = stream.collect(Collectors.groupingBy(e -> e.getMostSignificantBits())).entrySet().stream()
		//					.collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().stream()
		//							.collect(Collectors.summarizingLong(UUID::getLeastSignificantBits))));
		//		}
		//
		//		List<Map<String, Object>> syntheses = statsMap.entrySet().stream()
		//				.map(e -> new ImmutableMap.Builder<String, Object>().put("sensorType", e.getKey())
		//						.put("minValue", e.getValue().getMin()).put("maxValue", e.getValue().getMax())
		//						.put("mediumValue", Math.round(e.getValue().getAverage())).build())
		//				.collect(Collectors.toList());
		//
		//		return JsonFactory.create().toJson(syntheses);
	}

	@Override
	public void close() {
	}

	@Override
	public long getSize() {
		return indexReader.numDocs();
	}
}
