package com.capgemini.csd.hackaton.v2.bench;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;

import com.capgemini.csd.hackaton.Util;
import com.capgemini.csd.hackaton.client.AbstractClient;
import com.capgemini.csd.hackaton.v2.queue.Queue;
import com.capgemini.csd.hackaton.v2.queue.QueueMem;
import com.capgemini.csd.hackaton.v2.queue.QueueSpiderPig;
import com.google.common.base.Stopwatch;

import io.airlift.airline.Command;
import io.airlift.airline.Option;
import io.airlift.airline.OptionType;

@Command(name = "bench-queue", description = "Bench queue")
public class QueueBench implements Runnable {

	private static final int WARM_COUNT = 10000;

	@Option(type = OptionType.GLOBAL, name = { "-sp" }, description = "SpiderPig")
	protected boolean sp = false;

	@Option(type = OptionType.GLOBAL, name = { "-mem" }, description = "Mem")
	protected boolean mem = false;

	@Override
	public void run() {
		if (mem) {
			bench(new QueueMem());
		}
		if (sp) {
			bench(new QueueSpiderPig());
		}
	}

	private void bench(Queue queue) {
		String tmpDossier = getTmpDossier();
		queue.init(tmpDossier);

		queue.put(Util.messageFromJson(AbstractClient.getMessage(true)));
		queue.readMessage();

		for (int i = 0; i < WARM_COUNT; i++) {
			queue.put(Util.messageFromJson(AbstractClient.getMessage(true)));
		}
		for (int i = 0; i < WARM_COUNT; i++) {
			queue.readMessage();
		}
		Stopwatch sw = Stopwatch.createStarted();
		for (int j = 0; j < 100; j++) {
			for (int i = 0; i < 10000; i++) {
				queue.put(Util.messageFromJson(AbstractClient.getMessage(true)));
			}
			for (int i = 0; i < 10000; i++) {
				queue.readMessage();
			}
		}
		System.out.println(sw);
		queue.close();
		try {
			FileUtils.deleteDirectory(new File(tmpDossier));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private String getTmpDossier() {
		try {
			File tmpFile = File.createTempFile("bench", "queue");
			tmpFile.delete();
			tmpFile.mkdirs();
			return tmpFile.getAbsolutePath();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
}
