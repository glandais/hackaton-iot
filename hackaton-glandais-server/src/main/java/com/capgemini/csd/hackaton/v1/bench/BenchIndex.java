package com.capgemini.csd.hackaton.v1.bench;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.capgemini.csd.hackaton.client.AbstractClient;
import com.capgemini.csd.hackaton.v1.Commande;
import com.capgemini.csd.hackaton.v1.index.Index;
import com.google.common.base.Stopwatch;

import io.airlift.airline.Command;
import io.airlift.airline.Option;
import io.airlift.airline.OptionType;

@Command(name = "index", description = "Benchmark de l'index")
public class BenchIndex extends Commande {

	public final static Logger LOGGER = LoggerFactory.getLogger(BenchIndex.class);

	@Option(type = OptionType.COMMAND, name = "-n", description = "Nombre de messages")
	public int n = 10000;

	@Override
	public void execute() {
		Index index = getIndex();
		LOGGER.info("Implémentation de l'index : " + index.getClass().getCanonicalName());
		Stopwatch stopwatch = null;

		String message = AbstractClient.getMessage(true);
		try {
			index.index(message);
			index.index(message);
		} catch (Exception e) {
			LOGGER.error("Erreur", e);
		}
		LOGGER.info("Warmup");
		for (int i = -20000; i < n; i++) {
			if (i == 0) {
				LOGGER.info("Démarrage, indexation de " + n + " documents");
				stopwatch = Stopwatch.createStarted();
			}
			try {
				index.index(AbstractClient.getMessage(true));
			} catch (Exception e) {
				LOGGER.error("Erreur", e);
			}
		}
		LOGGER.info(n + " documents indexés en " + stopwatch);
		LOGGER.info(index.getSynthese());
		LOGGER.info(index.getSize() + " documents indexés.");
		index.close();

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
