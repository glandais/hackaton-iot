package com.capgemini.csd.hackaton.v1.index;

import org.mapdb.DB;
import org.mapdb.DBMaker;

public class IndexMapDBMemory extends AbstractIndexMapDB {

	@Override
	protected DB initDB(String dossier) {
		return DBMaker.memoryDB().make();		
	}

}
