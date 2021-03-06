package com.capgemini.csd.hackaton.v3;

import com.capgemini.csd.hackaton.v3.messages.Message;
import com.capgemini.csd.hackaton.v3.summaries.Summaries;

public interface Messages {

	public static final int MS_PAR_LOT = 1000;

	void add(Message message);

	Summaries getSummaries(long from, long to);

}
