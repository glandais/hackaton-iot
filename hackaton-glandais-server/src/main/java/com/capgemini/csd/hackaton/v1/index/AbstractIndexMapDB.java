package com.capgemini.csd.hackaton.v1.index;

import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.LongSummaryStatistics;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.boon.json.JsonFactory;
import org.mapdb.BTreeMap;
import org.mapdb.DB;
import org.mapdb.Serializer;

import com.capgemini.csd.hackaton.Util;
import com.google.common.collect.ImmutableMap;

public abstract class AbstractIndexMapDB implements Index {

	private DB db;
	private BTreeMap<UUID, UUID> map;
	private Set<String> ids;
	private static AtomicLong currentId = new AtomicLong();

	public AbstractIndexMapDB() {
		super();
	}

	@Override
	public void init(String dossier) {
		db = initDB(dossier);
		map = db.treeMap("messages").keySerializer(Serializer.UUID).valueSerializer(Serializer.UUID).createOrOpen();
		ids = db.treeSet("ids").serializer(Serializer.STRING_ASCII).createOrOpen();
	}

	protected abstract DB initDB(String dossier);

	@Override
	public boolean isInMemory() {
		return true;
	}

	@Override
	public void index(String json) {
		Map<?, ?> message = Util.fromJson(json);
		String id = (String) message.get("id");
		synchronized (ids) {
			if (ids.contains(id)) {
				throw new RuntimeException("ID existant : " + id);
			}
			ids.add(id);
		}
		long time = System.currentTimeMillis();
		long lo = time - 3600 * 1000;
		long timestamp = ((Date) message.get("timestamp")).getTime();
		if (timestamp > lo) {
			long sensorId = ((Number) message.get("sensorType")).longValue();
			long value = ((Number) message.get("value")).longValue();
			synchronized (map) {
				map.put(new UUID(timestamp, currentId.incrementAndGet()), new UUID(sensorId, value));
			}
		}
	}

	public static <T> Stream<T> asStream(Iterator<T> sourceIterator) {
		Iterable<T> iterable = () -> sourceIterator;
		return StreamSupport.stream(iterable.spliterator(), false);
	}

	@Override
	public String getSynthese() {
		long time = System.currentTimeMillis();
		long lo = time - 3600 * 1000;
		long hi = time;
		UUID from = new UUID(lo, 0);
		UUID to = new UUID(hi, 0);
		Iterator<UUID> iterator = map.descendingValueIterator(from, true, to, true);
		Stream<UUID> stream = asStream(iterator);

		Map<Long, LongSummaryStatistics> statsMap;
		statsMap = stream.collect(Collectors.groupingBy(e -> e.getMostSignificantBits())).entrySet().stream()
				.collect(Collectors.toMap(Map.Entry::getKey,
						e -> e.getValue().stream().collect(Collectors.summarizingLong(UUID::getLeastSignificantBits))));

		List<Map<String, Object>> syntheses = statsMap.entrySet().stream()
				.map(e -> new ImmutableMap.Builder<String, Object>().put("sensorType", e.getKey())
						.put("minValue", e.getValue().getMin()).put("maxValue", e.getValue().getMax())
						.put("mediumValue", Math.round(e.getValue().getAverage())).build())
				.collect(Collectors.toList());

		return JsonFactory.toJson(syntheses);
	}

	@Override
	public void close() {
		db.close();
	}

	@Override
	public long getSize() {
		return ids.size();
	}

}
