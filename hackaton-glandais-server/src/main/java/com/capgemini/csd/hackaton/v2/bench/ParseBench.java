package com.capgemini.csd.hackaton.v2.bench;

import org.boon.json.JsonFactory;

import com.capgemini.csd.hackaton.Util;
import com.capgemini.csd.hackaton.client.AbstractClient;
import com.capgemini.csd.hackaton.v2.message.Message;
import com.google.common.base.Stopwatch;

public class ParseBench {

	public static void main(String[] args) {
		// boon Map : 7.236 s
		// boon Message : 9.000 s
		Stopwatch sw = null;
		for (int i = -100000; i < 1000000; i++) {
			if (i == 0) {
				sw = Stopwatch.createStarted();
			}
			parseBoonMessage(AbstractClient.getMessage(true));
		}
		System.out.println(sw);
	}

	private static void parseBoonMessage(String message) {
		JsonFactory.fromJson(message, Message.class);
	}

	protected static void parseBoonMap(String message) {
		Util.fromJson(message);
	}

}
