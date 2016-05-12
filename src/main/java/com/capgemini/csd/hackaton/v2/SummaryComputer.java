package com.capgemini.csd.hackaton.v2;

import java.util.Map;

import com.capgemini.csd.hackaton.client.Summary;

public interface SummaryComputer {

	Map<Integer, Summary> getSummary(long timestamp, Integer duration);

}
