package com.capgemini.csd.hackaton.v2.store;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.LegacyIntField;
import org.apache.lucene.document.LegacyLongField;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SearcherFactory;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.NRTCachingDirectory;

import com.capgemini.csd.hackaton.client.Summary;
import com.capgemini.csd.hackaton.v2.message.Message;
import com.capgemini.csd.hackaton.v2.store.lucene.SummaryCollectorManager;

public class StoreLucene implements com.capgemini.csd.hackaton.v2.store.Store {

	private static final ExecutorService QUERY_EXECUTOR = Executors
			.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

	public static final FieldType TYPE_LONG = new FieldType();
	static {
		TYPE_LONG.setTokenized(false);
		TYPE_LONG.setIndexOptions(IndexOptions.NONE);
		TYPE_LONG.setNumericType(FieldType.LegacyNumericType.LONG);
		TYPE_LONG.setStored(true);
		TYPE_LONG.setDocValuesType(DocValuesType.NUMERIC);
		TYPE_LONG.freeze();
	}

	public static final FieldType TYPE_INT = new FieldType();
	static {
		TYPE_INT.setTokenized(false);
		TYPE_INT.setIndexOptions(IndexOptions.NONE);
		TYPE_INT.setNumericType(FieldType.LegacyNumericType.INT);
		TYPE_INT.setStored(true);
		TYPE_INT.setDocValuesType(DocValuesType.NUMERIC);
		TYPE_INT.freeze();
	}

	private Analyzer analyzer = new KeywordAnalyzer();

	private FSDirectory directory;

	private NRTCachingDirectory cachedFSDir;

	private IndexWriter iwriter;

	private double bufferSize;

	private SearcherManager searcherManager;

	private boolean multiThread;

	public StoreLucene(double bufferSize, boolean multiThread) {
		super();
		this.bufferSize = bufferSize;
		this.multiThread = multiThread;
	}

	@Override
	public void init(String dossier) {
		try {
			directory = FSDirectory.open(Paths.get(dossier, "lucene"));
			cachedFSDir = new NRTCachingDirectory(directory, 5.0, 60.0);
			IndexWriterConfig config = new IndexWriterConfig(analyzer);
			config.setOpenMode(OpenMode.CREATE_OR_APPEND);
			config.setRAMBufferSizeMB(bufferSize);
			iwriter = new IndexWriter(cachedFSDir, config);
			SearcherFactory sf;
			if (multiThread) {
				sf = new SearcherFactory() {
					@Override
					public IndexSearcher newSearcher(IndexReader reader, IndexReader previousReader)
							throws IOException {
						return new IndexSearcher(reader, QUERY_EXECUTOR);
					}
				};
			} else {
				sf = new SearcherFactory() {
					@Override
					public IndexSearcher newSearcher(IndexReader reader, IndexReader previousReader)
							throws IOException {
						return new IndexSearcher(reader);
					}
				};
			}
			searcherManager = new SearcherManager(iwriter, sf);
		} catch (IOException e) {
			throw new IllegalStateException("", e);
		}
	}

	@Override
	public void close() {
		try {
			iwriter.close();
			cachedFSDir.close();
			directory.close();
		} catch (IOException e) {
			throw new IllegalStateException("", e);
		}
	}

	@Override
	public Map<Integer, Summary> getSummary(long timestamp, Integer duration) {
		try {
			IndexSearcher isearcher = searcherManager.acquire();
			try {
				Query query = LongPoint.newRangeQuery("timestamp", timestamp, timestamp + 1000 * duration);
				SummaryCollectorManager collectorManager = new SummaryCollectorManager();
				return isearcher.search(query, collectorManager);
			} finally {
				searcherManager.release(isearcher);
			}
		} catch (IOException e) {
			throw new IllegalStateException("", e);
		}
	}

	@Override
	public boolean containsId(String id) {
		try {
			IndexSearcher isearcher = searcherManager.acquire();
			try {
				return isearcher.search(new TermQuery(new Term("id", id)), 1).totalHits > 0;
			} finally {
				searcherManager.release(isearcher);
			}
		} catch (IOException e) {
			throw new IllegalStateException("", e);
		}
	}

	@Override
	public void indexMessages(List<Message> messages) {
		try {
			Document doc = new Document();

			LegacyIntField sensorTypeField = new LegacyIntField("sensorType", 0, TYPE_INT);
			LegacyLongField valueField = new LegacyLongField("value", 0L, TYPE_LONG);
			LongPoint timestampField = new LongPoint("timestamp", 0L);
			StringField idField = new StringField("id", "", Store.NO);

			doc.add(sensorTypeField);
			doc.add(valueField);
			doc.add(timestampField);
			doc.add(idField);
			for (Message message : messages) {
				sensorTypeField.setIntValue(message.getSensorType());
				valueField.setLongValue(message.getValue());
				timestampField.setLongValue(message.getTimestamp());
				idField.setStringValue(message.getId());
				iwriter.addDocument(doc);
			}
			searcherManager.maybeRefresh();
		} catch (IOException e) {
			throw new IllegalStateException("", e);
		}
	}

}
