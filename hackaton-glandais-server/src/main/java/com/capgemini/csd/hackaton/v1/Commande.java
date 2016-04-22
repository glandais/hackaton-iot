package com.capgemini.csd.hackaton.v1;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.capgemini.csd.hackaton.client.Client;
import com.capgemini.csd.hackaton.server.Server;
import com.capgemini.csd.hackaton.v1.index.Index;
import com.capgemini.csd.hackaton.v1.queue.Queue;

import io.airlift.airline.Option;
import io.airlift.airline.OptionType;

public abstract class Commande implements Runnable {

	public final static Logger LOGGER = LoggerFactory.getLogger(Commande.class);

	@Option(type = OptionType.GLOBAL, name = { "--port", "-p" }, description = "Port")
	public int port = 8080;

	@Option(type = OptionType.GLOBAL, required = true, name = { "--dossier",
			"-d" }, description = "Dossier de test (supprimé au démarrage)")
	public String dossier;

	@Option(type = OptionType.GLOBAL, name = { "--client-class", "-clc" }, description = "Classe du client")
	public String clientClass = "com.capgemini.csd.hackaton.client.ClientAsyncHTTP";

	@Option(type = OptionType.GLOBAL, name = { "--server-class", "-sec" }, description = "Classe du serveur")
	public String serverClass = "com.capgemini.csd.hackaton.server.ServerNetty";

	@Option(type = OptionType.GLOBAL, name = { "--index-class", "-inc" }, description = "Classe de l'index")
	public String indexClass = "com.capgemini.csd.hackaton.index.IndexMemory";

	@Option(type = OptionType.GLOBAL, name = { "--queue-class", "-quc" }, description = "Classe de la queue")
	public String queueClass = "com.capgemini.csd.hackaton.queue.QueueChronicle";

	private Client client;

	private Server server;

	private Index index;

	private Queue queue;

	public Commande() {
		super();
	}

	private void init() {
		client = (Client) getIntance(clientClass);
		client.setHostPort("127.0.0.1", port);

		if (isServer()) {
			server = (Server) getIntance(serverClass);

			index = (Index) getIntance(indexClass);
			index.init(dossier);

			if (index instanceof Queue) {
				queue = (Queue) index;
			} else {
				queue = (Queue) getIntance(queueClass);
				queue.init(dossier);
			}
		}
	}

	public int getPort() {
		return port;
	}

	public String getDossier() {
		return dossier;
	}

	public String getClientClass() {
		return clientClass;
	}

	public String getServerClass() {
		return serverClass;
	}

	public Client getClient() {
		return client;
	}

	public Server getServer() {
		return server;
	}

	public Index getIndex() {
		return index;
	}

	public Queue getQueue() {
		return queue;
	}

	protected Object getIntance(String classe) {
		try {
			return Class.forName(classe).newInstance();
		} catch (Exception e) {
			throw new IllegalArgumentException(classe, e);
		}
	}

	@Override
	public void run() {
		if (isSupressionDossier()) {
			LOGGER.info("Suppression de " + dossier);
			File file = new File(dossier);
			FileUtils.deleteQuietly(file);
			if (file.exists()) {
				throw new IllegalStateException("Impossible de supprimer " + dossier);
			}
			file.mkdirs();
		}

		init();
		execute();
	}

	public abstract boolean isSupressionDossier();

	public abstract boolean isServer();

	public abstract void execute();
}
