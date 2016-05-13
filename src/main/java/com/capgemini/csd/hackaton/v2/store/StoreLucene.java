package com.capgemini.csd.hackaton.v2.store;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.FSDirectory;

import com.capgemini.csd.hackaton.client.Summary;
import com.capgemini.csd.hackaton.v2.message.Message;
import com.capgemini.csd.hackaton.v2.store.lucene.SummaryCollector;

public class StoreLucene implements com.capgemini.csd.hackaton.v2.store.Store {

	private Analyzer analyzer = new KeywordAnalyzer();

	private FSDirectory directory;

	private IndexWriter iwriter;

	private double bufferSize;

	public StoreLucene(double bufferSize) {
		super();
		this.bufferSize = bufferSize;
	}

	@Override
	public void init(String dossier) {
		try {
			directory = FSDirectory.open(Paths.get(dossier, "lucene"));
			IndexWriterConfig config = new IndexWriterConfig(analyzer);
			config.setOpenMode(OpenMode.CREATE_OR_APPEND);
			config.setRAMBufferSizeMB(bufferSize);
			iwriter = new IndexWriter(directory, config);
		} catch (IOException e) {
			throw new IllegalStateException("", e);
		}
	}

	@Override
	public void close() {
		try {
			iwriter.close();
			directory.close();
		} catch (IOException e) {
			throw new IllegalStateException("", e);
		}
	}

	@Override
	public Map<Integer, Summary> getSummary(long timestamp, Integer duration) {
		try {
			DirectoryReader ireader = DirectoryReader.open(iwriter);
			IndexSearcher isearcher = new IndexSearcher(ireader);
			Query query = LongPoint.newRangeQuery("timestamp", timestamp, timestamp + 1000 * duration);
			//			SummaryCollectorManager collectorManager = new SummaryCollectorManager(ireader);
			//			Map<Integer, Summary> summaries = isearcher.search(query, collectorManager);
			//			return summaries;
			SummaryCollector collector = new SummaryCollector(ireader);
			isearcher.search(query, collector);
			return collector.getSummaries();
		} catch (IOException e) {
			throw new IllegalStateException("", e);
		}
	}

	@Override
	public boolean containsId(String id) {
		try {
			DirectoryReader ireader = DirectoryReader.open(iwriter);
			IndexSearcher isearcher = new IndexSearcher(ireader);
			return isearcher.search(new TermQuery(new Term("id", id)), 1).totalHits > 0;
		} catch (IOException e) {
			throw new IllegalStateException("", e);
		}
	}

	@Override
	public void indexMessages(List<Message> messages) {
		try {
			List<Document> docs = new ArrayList<>(messages.size() + 1);
			for (Message message : messages) {
				Document doc = new Document();
				doc.add(new StoredField("sensorType", message.getSensorType()));
				doc.add(new StoredField("value", message.getValue()));
				doc.add(new LongPoint("timestamp", message.getTimestamp()));
				doc.add(new StringField("id", message.getId(), Store.NO));
				docs.add(doc);
			}
			iwriter.addDocuments(docs);
		} catch (IOException e) {
			throw new IllegalStateException("", e);
		}
	}

}
