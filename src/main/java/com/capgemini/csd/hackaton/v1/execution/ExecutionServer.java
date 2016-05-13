package com.capgemini.csd.hackaton.v1.execution;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.capgemini.csd.hackaton.Controler;
import com.capgemini.csd.hackaton.client.Client;
import com.capgemini.csd.hackaton.server.Server;
import com.capgemini.csd.hackaton.v1.Commande;
import com.capgemini.csd.hackaton.v1.index.Index;
import com.capgemini.csd.hackaton.v1.queue.Queue;

import io.airlift.airline.Command;

@Command(name = "server", description = "Serveur")
public class ExecutionServer extends Commande implements Controler {

	public final static Logger LOGGER = LoggerFactory.getLogger(ExecutionServer.class);

	private ReentrantLock lockRead = new ReentrantLock();

	private Queue queue;

	private Index index;

	private Server server;

	@Override
	public void execute() {
		this.queue = getQueue();
		this.index = getIndex();
		this.server = getServer();

		LOGGER.info("Implémentation de queue : " + queue.getClass().getCanonicalName());
		LOGGER.info("Implémentation de index : " + index.getClass().getCanonicalName());
		LOGGER.info("Implémentation de server : " + server.getClass().getCanonicalName());

		if (index.isInMemory()) {
			index();
			// int availableProcessors =
			// Runtime.getRuntime().availableProcessors();
			// Thread[] indexers = new Thread[availableProcessors];
			// for (int i = 0; i < availableProcessors; i++) {
			// indexers[i] = new Thread(() -> index());
			// indexers[i].start();
			// }
			// for (int i = 0; i < availableProcessors; i++) {
			// try {
			// indexers[i].join();
			// } catch (InterruptedException e) {
			// LOGGER.error("Died", e);
			// }
			// }
		}
		LOGGER.info(index.getSize() + " documents dans l'index.");

		// LOGGER.info("Initialisation du thread d'indexation.");
		// Thread thread = new Thread(() -> {
		// // boucle de traitement de la queue
		// while (true) {
		// String message = queue.getMessage();
		// if (message != null) {
		// while (message != null) {
		// try {
		// index.index(message, false);
		// } catch (Exception e) {
		// LOGGER.error("Echec à l'indexation de " + message);
		// }
		// message = queue.getMessage();
		// }
		// } else {
		// Sys.sleep(10L);
		// }
		// }
		// });
		// thread.setName("indexer");
		// thread.setPriority(Thread.MIN_PRIORITY);
		// thread.start();

		LOGGER.info("Démarrage du serveur");
		server.start(this, getPort());
		LOGGER.info("Serveur démarré");

		// warmup();

		LOGGER.info("Serveur prêt");
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				close();
			}
		});

		awaitTermination();
	}

	private void index() {
		LOGGER.info("Indexation initiale");
		String json = getMessage();
		long n = 0;
		while (json != null) {
			try {
				index.index(json);
				n++;
			} catch (Exception e) {
				LOGGER.error("Echec à l'indexation de " + json);
			}
			json = getMessage();
		}
		LOGGER.info(n + " messages indexés");
	}

	private String getMessage() {
		lockRead.lock();
		try {
			return queue.getMessage();
		} finally {
			lockRead.unlock();
		}
	}

	private void warmup() {
		LOGGER.info("Warm up");
		Client client = getClient();
		for (int i = 0; i < 20; i++) {
			client.sendMessages(5000, true);
		}
		client.shutdown();
	}

	@Override
	public void configure() {
		// TODO Auto-generated method stub

	}

	public String processRequest(String uri, Map<String, ? extends Collection<String>> params, String message)
			throws Exception {
		String result = "";
		try {
			if (uri.equals("/messages")) {
				index.index(message);
				queue.push(message);
				result = "OK";
			} else if (uri.equals("/messages/synthesis")) {
				result = index.getSynthese();
			} else if (uri.equals("/stop")) {
				close();
			}
		} catch (RuntimeException e) {
			LOGGER.error("?", e);
			throw new Exception(e);
		}
		return result;
	}

	public void close() {
		LOGGER.info("Fermeture");
		server.close();
		queue.close();
		index.close();
	}

	protected void awaitTermination() {
		server.awaitTermination();
	}

	@Override
	public boolean isSupressionDossier() {
		return false;
	}

	@Override
	public boolean isServer() {
		return true;
	}

}
