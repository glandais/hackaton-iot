package com.capgemini.csd.hackaton.v2.store;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.ArrayListHandler;
import org.h2.jdbcx.JdbcConnectionPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.capgemini.csd.hackaton.v2.synthese.Summary;

public class StoreH2 implements Store {

	public final static Logger LOGGER = LoggerFactory.getLogger(StoreH2.class);

	//	private static final String H2_OPTION = "nioMapped";
	private static final String H2_OPTION = "split:nioMapped";

	// stockage des messages
	private QueryRunner queryRunner;

	public void init(String dossier) {
		String h2Option = "";
		if (!System.getProperty("os.name").startsWith("Windows")) {
			h2Option = H2_OPTION + ":";
		}
		JdbcConnectionPool cp = JdbcConnectionPool.create("jdbc:h2:" + h2Option + dossier + "/messages", "sa", "sa");
		queryRunner = new QueryRunner(cp);
		try {
			queryRunner.update("SET AUTOCOMMIT TRUE");
			queryRunner.update(
					"CREATE TABLE IF NOT EXISTS MESSAGE (ID VARCHAR(64), TS TIMESTAMP, SENSORTYPE INT, VALUE BIGINT)");
			queryRunner.update("CREATE UNIQUE HASH INDEX IF NOT EXISTS IDXID ON MESSAGE(ID)");
			queryRunner.update("CREATE INDEX IF NOT EXISTS IDXTS ON MESSAGE(TS)");
			queryRunner.update("CREATE INDEX IF NOT EXISTS IDXST ON MESSAGE(SENSORTYPE)");
		} catch (Exception e) {
			LOGGER.error("", e);
		}

	}

	@Override
	public boolean containsId(String id) {
		try {
			return queryRunner.query("SELECT ID FROM MESSAGE WHERE ID = ?", rs -> rs.next(), id);
		} catch (SQLException e) {
			LOGGER.error("?", e);
			return false;
		}
	}

	@Override
	public void indexMessages(List<Map<String, Object>> messages) {
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
			LOGGER.error("?", e);
		}
	}

	@Override
	public Map<Integer, Summary> getSummary() {
		Map<Integer, Summary> res = new HashMap<>();
		try {
			List<Object[]> summaries = queryRunner.query(
					"SELECT SENSORTYPE, COUNT(*), SUM(VALUE), MIN(VALUE), MAX(VALUE) FROM MESSAGE "
							+ "WHERE TS > DATEADD('HOUR', -1, NOW()) AND TS < NOW() GROUP BY SENSORTYPE",
					new ArrayListHandler());
			res = summaries.stream()
					.collect(Collectors.toMap(a -> (Integer) a[0], a -> new Summary(((Number) a[0]).intValue(),
							(Number) a[1], (Number) a[2], (Number) a[3], (Number) a[4])));
		} catch (SQLException e) {
			LOGGER.error("?", e);
		}
		return res;
	}

}
