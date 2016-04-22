package com.capgemini.csd.hackaton;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.LongSummaryStatistics;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.boon.IO;
import org.boon.core.Sys;
import org.boon.json.JsonFactory;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.NodeBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.LongTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregation.SingleValue;
import org.elasticsearch.search.aggregations.metrics.max.Max;
import org.elasticsearch.search.aggregations.metrics.min.Min;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;
import org.mapdb.DBMaker;
import org.mapdb.IndexTreeList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.capgemini.csd.hackaton.server.IndexElasticSearch;
import com.capgemini.csd.hackaton.server.Server;
import com.capgemini.csd.hackaton.server.ServerNetty;

import io.airlift.airline.Cli;
import io.airlift.airline.Cli.CliBuilder;
import io.airlift.airline.Command;
import io.airlift.airline.Help;
import io.airlift.airline.Option;
import io.airlift.airline.OptionType;
import net.openhft.chronicle.queue.ChronicleQueueBuilder;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.TailerDirection;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.koloboke.collect.map.hash.HashObjObjMaps;

@Command(name = "server", description = "Serveur")
public class IOTServer implements Runnable, Controler {

	public final static Logger LOGGER = LoggerFactory.getLogger(IOTServer.class);

	@Option(type = OptionType.GLOBAL, name = { "--port", "-p" }, description = "Port")
	protected int port = 80;

	@Option(type = OptionType.GLOBAL, name = { "--dossier", "-d" }, description = "Dossier")
	protected String dossier = "/var/glandais";

	// composant serveur
	protected Server server;

	// queue des éléments à persisté
	protected SingleChronicleQueue queueToPersist;

	// messages reçus non stockés par ES
	private NavigableMap<UUID, UUID> memoryMap;

	// correspondance id du message/UUID de la memoryMap
	private Map<Object, Object> memoryIds;

	// id incrémental, si deux messages avec le même timestamp
	private static AtomicLong currentId = new AtomicLong();

	// blocage indexation/calcul de la synthèse
	private ReentrantLock indexLock = new ReentrantLock();

	// stockage des messages
	private Client es;

	private IndexTreeList<Object> marker;

	public static void main(String[] args) {
		CliBuilder<Runnable> builder = Cli.<Runnable> builder("hackaton-glandais").withDefaultCommand(IOTServer.class)
				.withCommands(IOTServer.class, Help.class);

		Cli<Runnable> parser = builder.build();
		parser.parse(args).run();
	}

	@Override
	public void run() {
		dossier = new File(dossier).getAbsolutePath();
		LOGGER.info("Dossier : " + dossier);

		memoryMap = Collections.synchronizedNavigableMap(new TreeMap<>());
		memoryIds = Collections.synchronizedMap(HashObjObjMaps.newMutableMap());

		es = NodeBuilder.nodeBuilder().local(true)
				.settings(Settings.builder().put("http.enabled", "true").put("path.home", dossier + "/es")).node()
				.client();
		String mapping = IO.read(IndexElasticSearch.class.getResourceAsStream("/message.json"));
		try {
			boolean aliasExists = es.admin().indices().prepareExists("iot").execute().get().isExists();
			if (!aliasExists) {
				es.admin().indices().prepareCreate("iot")
						.setSettings(Settings.builder().put("number_of_shards", 1).put("number_of_replicas", 0))
						.addMapping("message", mapping).get();
			}
		} catch (Exception e) {
			LOGGER.error("...", e);
		}

		marker = DBMaker.fileDB(dossier + "/marker").make().indexTreeList("marker").createOrOpen();

		// liste de tous les messages
		queueToPersist = ChronicleQueueBuilder.single(dossier + "/queueToPersist").build();

		// récupération de l'id du dernier message enregistré
		String lastPersistedId = marker.size() > 0 ? (String) marker.get(0) : null;

		// enregistrement des messages non enregistrés
		ExcerptTailer tailerToPersist = queueToPersist.createTailer();
		//		tailerToPersist.readText();
		tailerToPersist.direction(TailerDirection.BACKWARD).toEnd();
		index(tailerToPersist, lastPersistedId, true);

		// FIXME RAZ queueToPersist quand tout a été persisté

		startPersister();

		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				close();
			}
		});

		this.server = new ServerNetty();
		server.start(this, port);
		LOGGER.info("Serveur démarré");
		server.awaitTermination();
	}

	@Override
	public String processRequest(String uri, String message) throws Exception {
		String result = "";
		try {
			if (uri.equals("/messages")) {
				process(message);
				result = "OK";
			} else if (uri.equals("/messages/synthesis")) {
				result = getSynthese();
			} else if (uri.equals("/stop")) {
				close();
			}
		} catch (RuntimeException e) {
			LOGGER.error("?", e);
			throw new Exception(e);
		}
		return result;
	}

	protected void close() {
		LOGGER.info("Fermeture");
		server.close();
	}

	private void process(String json) {
		Map<String, Object> message = JsonFactory.fromJson(json, Map.class);
		String id = (String) message.get("id");
		UUID uuid = null;
		if (memoryIds.containsKey(id)) {
			throw new RuntimeException("ID existant : " + id);
		}
		long timestamp = ((Date) message.get("timestamp")).getTime();
		message.put("timestamp", Long.valueOf(timestamp));
		uuid = new UUID(timestamp, currentId.incrementAndGet());
		memoryIds.put(id, uuid);

		SearchRequestBuilder search = es.prepareSearch("iot").setTypes("message");
		search.setQuery(QueryBuilders.termQuery("id", id));
		search.setSize(0);
		SearchResponse result = search.execute().actionGet();
		if (result.getHits().getTotalHits() > 0) {
			throw new RuntimeException("ID existant : " + id);
		}

		long sensorId = ((Number) message.get("sensorType")).longValue();
		long value = ((Number) message.get("value")).longValue();
		memoryMap.put(uuid, new UUID(sensorId, value));

		// mise en queue pour la persistence
		queueToPersist.createAppender().writeText(json);
	}

	private String getSynthese() {
		indexLock.lock();
		try {

			SearchRequestBuilder search = es.prepareSearch("iot").setTypes("message");
			search.setQuery(QueryBuilders.rangeQuery("timestamp").from("now-1h").to("now"));
			search.setSize(0);
			TermsBuilder groupBy = AggregationBuilders.terms("group_by").field("sensorType");
			groupBy.subAggregation(AggregationBuilders.sum("sum").field("value"));
			groupBy.subAggregation(AggregationBuilders.count("count").field("value"));
			groupBy.subAggregation(AggregationBuilders.min("min").field("value"));
			groupBy.subAggregation(AggregationBuilders.max("max").field("value"));
			search.addAggregation(groupBy);
			SearchResponse result = search.execute().actionGet();
			LongTerms aggregation = result.getAggregations().get("group_by");
			Map<Integer, Map<String, Object>> syntheses = new HashMap<>();
			for (Bucket bucket : aggregation.getBuckets()) {
				Map<String, Object> synthesis = new HashMap<>();
				Integer id = Integer.valueOf(bucket.getKeyAsString());
				synthesis.put("sensorType", id);
				Sum sum = bucket.getAggregations().get("sum");
				SingleValue count = bucket.getAggregations().get("count");
				Min min = bucket.getAggregations().get("min");
				Max max = bucket.getAggregations().get("max");
				synthesis.put("minValue", Math.round(min.getValue()));
				synthesis.put("maxValue", Math.round(max.getValue()));
				synthesis.put("sum", Math.round(sum.getValue()));
				synthesis.put("count", Math.round(count.value()));
				syntheses.put(id, synthesis);
			}

			long time = System.currentTimeMillis();
			long lo = time - 3600 * 1000;
			long hi = time;
			UUID from = new UUID(lo, 0);
			UUID to = new UUID(hi, 0);
			Stream<UUID> stream = memoryMap.subMap(from, to).values().stream();

			Map<Long, LongSummaryStatistics> statsMap = stream
					.collect(Collectors.groupingBy(e -> e.getMostSignificantBits())).entrySet().stream()
					.collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().stream()
							.collect(Collectors.summarizingLong(UUID::getLeastSignificantBits))));

			//			List<Map<String, Object>> syntheses = statsMap.entrySet().stream()
			//					.map(e -> new ImmutableMap.Builder<String, Object>().put("sensorType", e.getKey())
			//							.put("minValue", e.getValue().getMin()).put("maxValue", e.getValue().getMax())
			//							.put("mediumValue", Math.round(e.getValue().getAverage())).build())
			//					.collect(Collectors.toList());

		} finally {
			indexLock.unlock();
		}
		return "";
	}

	private void startPersister() {
		Thread thread = new Thread(() -> {
			ExcerptTailer tailerToPersist = queueToPersist.createTailer();
			tailerToPersist.toEnd();
			while (true) {
				index(tailerToPersist, null, false);
				Sys.sleep(5000L);
			}
		});
		thread.setName("persister");
		thread.setPriority(Thread.MIN_PRIORITY);
		thread.start();
	}

	private void index(ExcerptTailer tailerToPersist, String lastIndexed, boolean initial) {
		List<Map<String, Object>> messages = new ArrayList<>(1001);

		String message = tailerToPersist.readText();
		String precedent = null;
		if (initial && message != null) {
			Map<String, Object> map = JsonFactory.fromJson(message, Map.class);
			if (marker.size() == 0) {
				marker.add(map.get("id"));
			} else {
				marker.set(0, map.get("id"));
			}
		}
		while (message != null) {
			Map<String, Object> map = JsonFactory.fromJson(message, Map.class);
			if (lastIndexed != null && lastIndexed.equals(map.get("id"))) {
				break;
			}
			messages.add(map);
			if (messages.size() == 1000) {
				indexMessages(messages, initial);
				messages = new ArrayList<>(1001);
			}
			if (message.equals(precedent)) {
				break;
			}
			precedent = message;
			message = tailerToPersist.readText();
		}
		if (messages.size() > 0) {
			indexMessages(messages, initial);
		}
	}

	private void indexMessages(List<Map<String, Object>> messages, boolean initial) {
		indexLock.lock();
		try {
			BulkRequestBuilder bulkRequest = es.prepareBulk();
			for (Map<String, Object> map : messages) {
				bulkRequest.add(es.prepareIndex("iot", "message").setSource(map));
				Object uuid = memoryIds.remove(map.get("id"));
				if (uuid != null) {
					memoryMap.remove(uuid);
				}
			}
			bulkRequest.execute().actionGet();
			if (!initial) {
				String id = (String) messages.get(messages.size() - 1).get("id");
				if (marker.size() == 0) {
					marker.add(id);
				} else {
					marker.set(0, id);
				}
			}
		} finally {
			indexLock.unlock();
		}
	}

}
