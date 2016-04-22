package com.capgemini.csd.hackaton.v1.bench;

import java.util.concurrent.atomic.AtomicLong;

import org.boon.core.Sys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.capgemini.csd.hackaton.client.AbstractClient;
import com.capgemini.csd.hackaton.v1.Commande;
import com.capgemini.csd.hackaton.v1.queue.Queue;
import com.google.common.base.Stopwatch;

import io.airlift.airline.Command;
import io.airlift.airline.Option;
import io.airlift.airline.OptionType;

@Command(name = "queue", description = "Benchmark de la queue")
public class BenchQueue extends Commande {

	public final static Logger LOGGER = LoggerFactory.getLogger(BenchQueue.class);

	@Option(type = OptionType.COMMAND, name = "-n", description = "Nombre de messages")
	public int n = 1000000;

	protected AtomicLong tot = new AtomicLong();

	@Override
	public void execute() {
		Queue queue = getQueue();
		LOGGER.info("Implémentation de queue : " + queue.getClass().getCanonicalName());

		Thread thread = new Thread(() -> {
			// boucle de traitement de la queue
			while (true) {
				String json = queue.getMessage();
				if (json != null) {
					while (json != null) {
						tot.incrementAndGet();
						json = queue.getMessage();
					}
				} else {
					Sys.sleep(10L);
				}
			}
		});
		thread.setName("indexer");
		thread.setPriority(Thread.MIN_PRIORITY);
		thread.start();

		Stopwatch stopwatch = null;

		LOGGER.info("Warmup");
		for (int i = -20000; i < n; i++) {
			if (i == 0) {
				LOGGER.info("Démarrage, mise en queue de " + n + " documents");
				stopwatch = Stopwatch.createStarted();
			}
			queue.push(AbstractClient.getMessage(true));
		}
		LOGGER.info(n + " documents pushés en " + stopwatch);
		while (tot.get() != n + 20000) {
			Sys.sleep(1L);
		}
		LOGGER.info(n + " documents lus en " + stopwatch);
		queue.close();

		System.exit(0);
	}

	@Override
	public boolean isSupressionDossier() {
		return true;
	}

	@Override
	public boolean isServer() {
		return true;
	}
}
