package com.capgemini.csd.hackaton.v2.store;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.ArrayListHandler;
import org.h2.jdbcx.JdbcConnectionPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.capgemini.csd.hackaton.client.Summary;
import com.capgemini.csd.hackaton.v2.message.Message;

public class StoreH2Mem implements Store {

	public final static Logger LOGGER = LoggerFactory.getLogger(StoreH2Mem.class);

	//	private static final String H2_OPTION = "nioMapped";
	private static final String H2_OPTION = "split:nioMapped";

	// stockage des messages
	private QueryRunner queryRunnerMem;

	// stockage des messages
	private QueryRunner queryRunnerDisk;

	public void init(String dossier) {
		String h2Option = "";
		if (!System.getProperty("os.name").startsWith("Windows")) {
			h2Option = H2_OPTION + ":";
		}

		JdbcConnectionPool cpMem = JdbcConnectionPool.create("jdbc:h2:mem:dbMem", "sa", "sa");
		queryRunnerMem = new QueryRunner(cpMem);
		try {
			queryRunnerMem.update("SET AUTOCOMMIT TRUE");
			queryRunnerMem.update("CREATE MEMORY TABLE IF NOT EXISTS IDS (ID VARCHAR(64) PRIMARY KEY HASH)");
			queryRunnerMem.update("CREATE UNIQUE HASH INDEX IF NOT EXISTS IDXID ON IDS(ID)");
		} catch (Exception e) {
			LOGGER.error("", e);
		}

		JdbcConnectionPool cpDisk = JdbcConnectionPool.create("jdbc:h2:" + h2Option + dossier + "/messages", "sa",
				"sa");
		queryRunnerDisk = new QueryRunner(cpDisk);
		try {
			queryRunnerDisk.update("SET AUTOCOMMIT TRUE");
			queryRunnerDisk.update(
					"CREATE TABLE IF NOT EXISTS MESSAGE (ID VARCHAR(64), TS BIGINT, SENSORTYPE INT, VALUE BIGINT)");
			queryRunnerDisk.update("CREATE INDEX IF NOT EXISTS IDXTS ON MESSAGE(TS)");
			queryRunnerDisk.update("CREATE INDEX IF NOT EXISTS IDXST ON MESSAGE(SENSORTYPE)");
		} catch (Exception e) {
			LOGGER.error("", e);
		}

		try {
			queryRunnerDisk.query("SELECT ID FROM MESSAGE", rs -> {
				Object[][] ids = new Object[1000][1];
				int i = 0;
				while (rs.next()) {
					ids[i++][0] = rs.getString(1);
					if (i == 1000) {
						insertIds(ids, 1000);
						i = 0;
					}
				}
				if (i != 0) {
					insertIds(ids, i);
				}
				return null;
			});
		} catch (Exception e) {
			LOGGER.error("", e);
		}

	}

	private void insertIds(Object[][] paramsIds, int count) throws SQLException {
		Object[][] ids = paramsIds;
		if (count != paramsIds.length) {
			ids = Arrays.copyOfRange(paramsIds, 0, count);
		}
		queryRunnerMem.batch("INSERT INTO IDS (ID) VALUES (?)", ids);
	}

	@Override
	public boolean containsId(String id) {
		try {
			return queryRunnerMem.query("SELECT ID FROM IDS WHERE ID = ?", rs -> rs.next(), id);
		} catch (SQLException e) {
			LOGGER.error("?", e);
			return false;
		}
	}

	@Override
	public void indexMessages(List<Message> messages) {
		Object[][] params = new Object[messages.size()][4];
		Object[][] paramsIds = new Object[messages.size()][1];
		for (int i = 0; i < messages.size(); i++) {
			Object id = messages.get(i).getId();
			params[i][0] = id;
			paramsIds[i][0] = id;
			params[i][1] = messages.get(i).getTimestamp();
			params[i][2] = messages.get(i).getSensorType();
			params[i][3] = messages.get(i).getValue();
		}
		try {
			queryRunnerDisk.batch("INSERT INTO MESSAGE (ID, TS, SENSORTYPE, VALUE) VALUES (?, ?, ?, ?)", params);
			queryRunnerMem.batch("INSERT INTO IDS (ID) VALUES (?)", paramsIds);
		} catch (SQLException e) {
			LOGGER.error("?", e);
		}
	}

	@Override
	public Map<Integer, Summary> getSummary(long timestamp, Integer duration) {
		Map<Integer, Summary> res = new HashMap<>();
		try {
			List<Object[]> summaries = queryRunnerDisk.query(
					"SELECT SENSORTYPE, COUNT(*), SUM(VALUE), MIN(VALUE), MAX(VALUE) FROM MESSAGE "
							+ "WHERE TS > ? AND TS < ? GROUP BY SENSORTYPE",
					new ArrayListHandler(), new Date(timestamp), new Date(timestamp + duration * 1000));
			res = summaries.stream()
					.collect(Collectors.toMap(a -> (Integer) a[0], a -> new Summary(((Number) a[0]).intValue(),
							(Number) a[1], (Number) a[2], (Number) a[3], (Number) a[4])));
		} catch (SQLException e) {
			LOGGER.error("?", e);
		}
		return res;
	}

}
