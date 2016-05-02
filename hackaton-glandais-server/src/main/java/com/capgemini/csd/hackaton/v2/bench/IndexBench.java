package com.capgemini.csd.hackaton.v2.bench;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import org.apache.commons.dbutils.QueryRunner;
import org.h2.jdbcx.JdbcConnectionPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Stopwatch;

import net.openhft.koloboke.collect.map.hash.HashObjObjMaps;

public class IndexBench {

	public final static Logger LOGGER = LoggerFactory.getLogger(IndexBench.class);

	private static final int SIZE = 10000;

	public final static Random R = new Random();

	public static void main(String[] args) throws SQLException {
		Map<String, String> ids = getIds();
		hashSet(ids);
		hashH2(ids, 0);
		hashSet(ids);
		hashH2(ids, 1);
	}

	private static void hashH2(Map<String, String> ids, int j) throws SQLException {
		JdbcConnectionPool cpMem = JdbcConnectionPool.create("jdbc:h2:mem:dbMem" + j, "sa", "sa");
		QueryRunner queryRunnerMem = new QueryRunner(cpMem);
		try {
			queryRunnerMem.update("SET AUTOCOMMIT TRUE");
			queryRunnerMem.update("CREATE MEMORY TABLE IF NOT EXISTS IDS (ID VARCHAR(64) PRIMARY KEY HASH)");
			queryRunnerMem.update("CREATE UNIQUE HASH INDEX IF NOT EXISTS IDXID ON IDS(ID)");
		} catch (Exception e) {
			LOGGER.error("", e);
		}

		List<String> lids = new ArrayList<>(ids.keySet());
		Stopwatch stopwatch = Stopwatch.createStarted();
		Object[][] oids = new Object[1000][1];
		int i = 0;
		int k = 0;
		while (k < lids.size()) {
			oids[i++][0] = lids.get(k);
			if (i == 1000) {
				insertIds(queryRunnerMem, oids, 1000);
				i = 0;
			}
			k++;
		}
		if (i != 0) {
			insertIds(queryRunnerMem, oids, i);
		}
		System.out.println("hashH2 index " + stopwatch.toString());
		stopwatch.reset();
		stopwatch.start();
		for (int l = 0; l < SIZE; l++) {
			if (l % 2 == 0) {
				queryRunnerMem.query("SELECT ID FROM IDS WHERE ID = ?", rs -> rs.next(), getId());
			} else {
				queryRunnerMem.query("SELECT ID FROM IDS WHERE ID = ?", rs -> rs.next(),
						lids.get(R.nextInt(lids.size())));
			}
		}
		System.out.println("hashH2 contains " + stopwatch.toString());
	}

	private static void insertIds(QueryRunner queryRunnerMem, Object[][] paramsIds, int count) throws SQLException {
		Object[][] ids = paramsIds;
		if (count != paramsIds.length) {
			ids = Arrays.copyOfRange(paramsIds, 0, count);
		}
		queryRunnerMem.batch("INSERT INTO IDS (ID) VALUES (?)", ids);
	}

	private static Map<String, String> getIds() {
		Map<String, String> res = new HashMap<String, String>(SIZE);
		for (int i = 0; i < SIZE; i++) {
			res.put(getId(), getId());
		}
		return res;
	}

	protected static String getId() {
		return UUID.randomUUID().toString().replaceAll("-", "") + UUID.randomUUID().toString().replaceAll("-", "");
	}

	private static void hashSet(Map<String, String> ids) {
		Map<String, String> index = Collections.synchronizedMap(HashObjObjMaps.newMutableMap());

		Stopwatch stopwatch = Stopwatch.createStarted();
		index.putAll(ids);
		System.out.println("hashSet index " + stopwatch.toString());
		stopwatch.reset();
		stopwatch.start();
		for (int l = 0; l < SIZE; l++) {
			if (l % 2 == 0) {
				index.keySet().contains(getId());
			} else {
				index.keySet().contains(ids.get(R.nextInt(ids.size())));
			}
		}
		System.out.println("hashSet contains " + stopwatch.toString());
	}

}
