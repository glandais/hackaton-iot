package com.capgemini.csd.hackaton.v2.store;

import java.util.List;
import java.util.Map;

import com.capgemini.csd.hackaton.v2.SummaryComputer;

public interface Store extends SummaryComputer {

	boolean containsId(String id);

	void indexMessages(List<Map<String, Object>> messages);

}
