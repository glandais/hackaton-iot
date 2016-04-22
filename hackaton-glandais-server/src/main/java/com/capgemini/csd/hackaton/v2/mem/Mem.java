package com.capgemini.csd.hackaton.v2.mem;

import java.util.List;
import java.util.Map;

import com.capgemini.csd.hackaton.v2.SummaryComputer;

public interface Mem extends SummaryComputer {

	boolean containsId(String id);

	void removeMessages(List<Map<String, Object>> messages);

	void putId(String id);

	void index(Map<String, Object> message);

	long getSize();

}
