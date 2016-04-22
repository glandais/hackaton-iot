package com.capgemini.csd.hackaton;

import java.io.File;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.LongSummaryStatistics;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.ArrayHandler;
import org.boon.core.Sys;
import org.boon.json.JsonFactory;
import org.h2.jdbcx.JdbcConnectionPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
public class IOTServerH2 implements Runnable, Controler {

	//	private static final String H2_OPTION = "nioMapped";
	private static final String H2_OPTION = "split:nioMapped";

	private AtomicInteger processing = new AtomicInteger();

	private static final int BATCH_SIZE = 1000;

	public final static Logger LOGGER = LoggerFactory.getLogger(IOTServerH2.class);

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
	private QueryRunner queryRunner;

	public static void main(String[] args) {
		CliBuilder<Runnable> builder = Cli.<Runnable> builder("hackaton-glandais").withDefaultCommand(IOTServerH2.class)
				.withCommands(IOTServerH2.class, Help.class);

		Cli<Runnable> parser = builder.build();
		parser.parse(args).run();
	}

	@Override
	public void run() {
		dossier = new File(dossier).getAbsolutePath();
		LOGGER.info("Dossier : " + dossier);

		memoryMap = Collections.synchronizedNavigableMap(new TreeMap<>());
		memoryIds = Collections.synchronizedMap(HashObjObjMaps.newMutableMap());

		JdbcConnectionPool cp = JdbcConnectionPool.create("jdbc:h2:" + H2_OPTION + ":" + dossier + "/messages", "sa",
				"sa");
		queryRunner = new QueryRunner(cp);
		try {
			queryRunner.update("SET AUTOCOMMIT TRUE");
			queryRunner.update("CREATE TABLE IF NOT EXISTS LAST_MESSAGE (ID VARCHAR(64), TS TIMESTAMP)");
			queryRunner.update("CREATE INDEX IF NOT EXISTS IDXLID ON LAST_MESSAGE(ID)");
			queryRunner.update("CREATE INDEX IF NOT EXISTS IDXLTS ON LAST_MESSAGE(TS)");
			queryRunner.update(
					"CREATE TABLE IF NOT EXISTS MESSAGE (ID VARCHAR(64), TS TIMESTAMP, SENSORTYPE INT, VALUE BIGINT)");
			queryRunner.update("CREATE INDEX IF NOT EXISTS IDXID ON MESSAGE(ID)");
			queryRunner.update("CREATE INDEX IF NOT EXISTS IDXTS ON MESSAGE(TS)");
			queryRunner.update("CREATE INDEX IF NOT EXISTS IDXST ON MESSAGE(SENSORTYPE)");
		} catch (Exception e) {
			LOGGER.error("", e);
		}

		// liste de tous les messages
		queueToPersist = ChronicleQueueBuilder.single(dossier + "/queueToPersist").build();

		// récupération de l'id du dernier message enregistré
		String lastPersistedId = null;
		try {
			Object[] result = queryRunner.query("SELECT ID FROM LAST_MESSAGE ORDER BY TS DESC LIMIT 1",
					new ArrayHandler());
			if (result != null && result.length > 0) {
				lastPersistedId = (String) result[0];
			}
			queryRunner.update("DELETE FROM LAST_MESSAGE");
		} catch (SQLException e) {
			lastPersistedId = null;
			LOGGER.error("", e);
		}

		// enregistrement des messages non enregistrés
		ExcerptTailer tailerToPersist = queueToPersist.createTailer().direction(TailerDirection.BACKWARD).toEnd();
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
				try {
					processing.incrementAndGet();
					process(message);
				} finally {
					processing.decrementAndGet();
				}
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
		long timestamp = ((Date) message.get("timestamp")).getTime();
		UUID uuid = new UUID(timestamp, currentId.incrementAndGet());

		indexLock.lock();
		try {
			try {
				if (memoryIds.containsKey(id)
						|| queryRunner.query("SELECT ID FROM MESSAGE WHERE ID = ?", rs -> rs.next(), id)) {
					throw new RuntimeException("ID existant : " + id);
				}
			} catch (SQLException e) {
				throw new RuntimeException("?", e);
			}
			memoryIds.put(id, uuid);
		} finally {
			indexLock.unlock();
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

			//			SearchRequestBuilder search = es.prepareSearch("iot").setTypes("message");
			//			search.setQuery(QueryBuilders.rangeQuery("timestamp").from("now-1h").to("now"));
			//			search.setSize(0);
			//			TermsBuilder groupBy = AggregationBuilders.terms("group_by").field("sensorType");
			//			groupBy.subAggregation(AggregationBuilders.sum("sum").field("value"));
			//			groupBy.subAggregation(AggregationBuilders.count("count").field("value"));
			//			groupBy.subAggregation(AggregationBuilders.min("min").field("value"));
			//			groupBy.subAggregation(AggregationBuilders.max("max").field("value"));
			//			search.addAggregation(groupBy);
			//			SearchResponse result = search.execute().actionGet();
			//			LongTerms aggregation = result.getAggregations().get("group_by");
			//			Map<Integer, Map<String, Object>> syntheses = new HashMap<>();
			//			for (Bucket bucket : aggregation.getBuckets()) {
			//				Map<String, Object> synthesis = new HashMap<>();
			//				Integer id = Integer.valueOf(bucket.getKeyAsString());
			//				synthesis.put("sensorType", id);
			//				Sum sum = bucket.getAggregations().get("sum");
			//				SingleValue count = bucket.getAggregations().get("count");
			//				Min min = bucket.getAggregations().get("min");
			//				Max max = bucket.getAggregations().get("max");
			//				synthesis.put("minValue", Math.round(min.getValue()));
			//				synthesis.put("maxValue", Math.round(max.getValue()));
			//				synthesis.put("sum", Math.round(sum.getValue()));
			//				synthesis.put("count", Math.round(count.value()));
			//				syntheses.put(id, synthesis);
			//			}

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
				if (processing.get() == 0) {
					index(tailerToPersist, null, false);
				} else {
					Sys.sleep(5000L);
				}
			}
		});
		thread.setName("persister");
		thread.setPriority(Thread.MIN_PRIORITY);
		thread.start();
	}

	private void index(ExcerptTailer tailerToPersist, String lastIndexed, boolean initial) {
		List<Map<String, Object>> messages = new ArrayList<>(BATCH_SIZE + 1);

		String message = tailerToPersist.readText();
		String precedent = null;
		if (initial && message != null) {
			Map<String, Object> map = JsonFactory.fromJson(message, Map.class);
			insertId((String) map.get("id"));
		}
		while (message != null) {
			Map<String, Object> map = JsonFactory.fromJson(message, Map.class);
			if (lastIndexed != null && lastIndexed.equals(map.get("id"))) {
				break;
			}
			messages.add(map);
			if (messages.size() == BATCH_SIZE) {
				indexMessages(messages, initial);
				messages = new ArrayList<>(BATCH_SIZE + 1);
				if (!initial) {
					break;
				}
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
			Object[][] params = new Object[messages.size()][4];
			for (int i = 0; i < messages.size(); i++) {
				params[i][0] = messages.get(i).get("id");
				params[i][1] = messages.get(i).get("timestamp");
				params[i][2] = messages.get(i).get("sensorType");
				params[i][3] = messages.get(i).get("value");
			}
			try {
				queryRunner.batch("INSERT INTO MESSAGE (ID, TS, SENSORTYPE, VALUE) VALUES (?, ?, ?, ?)", params);
			} catch (SQLException e) {
				throw new RuntimeException(e);
			}
			for (int i = 0; i < messages.size(); i++) {
				Object uuid = memoryIds.remove(params[i][0]);
				if (uuid != null) {
					memoryMap.remove(uuid);
				}
			}
			if (!initial) {
				String id = (String) messages.get(messages.size() - 1).get("id");
				insertId(id);
			}
		} finally {
			indexLock.unlock();
		}
	}

	private void insertId(String id) {
		try {
			queryRunner.insert("INSERT INTO LAST_MESSAGE (ID, TS) VALUES (?, ?)", rs -> null, id, new Date());
		} catch (SQLException e) {
			LOGGER.error("", e);
		}
	}

}
