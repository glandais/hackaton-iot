package com.capgemini.csd.hackaton.v2.bench;

import java.util.Map;
import java.util.function.Function;

import org.boon.json.JsonFactory;

import com.capgemini.csd.hackaton.Util;
import com.capgemini.csd.hackaton.client.AbstractClient;
import com.capgemini.csd.hackaton.v2.message.Message;
import com.google.common.base.Stopwatch;

import io.airlift.airline.Command;

@Command(name = "bench-parse", description = "Bench parse")
public class ParseBench implements Runnable {

	public static void main(String[] args) {
		new ParseBench().run();
	}

	@Override
	public void run() {
		bench("Parse to bean 2 ", ParseBench::parseMoshi);
		bench("Parse to bean ", ParseBench::parseBoonMessage);
		bench("Parse to map ", ParseBench::parseBoonMap);
		bench("Parse to bean 2 ", ParseBench::parseMoshi);
		bench("Parse to bean ", ParseBench::parseBoonMessage);
		bench("Parse to map ", ParseBench::parseBoonMap);
	}

	protected void bench(String message, Function<String, ?> f) {
		Stopwatch sw = null;
		for (int i = -100000; i < 1000000; i++) {
			if (i == 0) {
				sw = Stopwatch.createStarted();
			}
			f.apply(AbstractClient.getMessage(true));
		}
		System.out.println(message + " " + sw);
	}

	public static Message parseBoonMessage(String message) {
		return JsonFactory.fromJson(message, Message.class);
	}

	public static Message parseMoshi(String message) {
		return Util.messageFromJson(message);
	}

	public static Map parseBoonMap(String message) {
		return Util.fromJson(message);
	}

}
