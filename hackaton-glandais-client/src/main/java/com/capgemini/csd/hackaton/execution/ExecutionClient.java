package com.capgemini.csd.hackaton.execution;

import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.boon.json.JsonFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.capgemini.csd.hackaton.client.AbstractClient;
import com.capgemini.csd.hackaton.client.Client;

import io.airlift.airline.Command;
import io.airlift.airline.Option;
import io.airlift.airline.OptionType;

@Command(name = "client", description = "Client")
public class ExecutionClient implements Runnable {

	public final static Logger LOGGER = LoggerFactory.getLogger(ExecutionClient.class);

	@Option(type = OptionType.GLOBAL, name = { "--host", "-h" }, description = "Host")
	public String host = "192.168.1.1";

	@Option(type = OptionType.GLOBAL, name = { "--port", "-p" }, description = "Port")
	public int port = 80;

	@Option(type = OptionType.COMMAND, name = "-n", description = "Nombre de lots de messages")
	public int n = 1000;

	@Option(type = OptionType.COMMAND, name = "-m", description = "Nombre de messages par lot")
	public int m = 1000;

	@Option(type = OptionType.GLOBAL, name = { "--client-class", "-clc" }, description = "Classe du client")
	public String clientClass = "com.capgemini.csd.hackaton.client.ClientAsyncHTTP";

	@Option(type = OptionType.COMMAND, name = "-l", description = "Tests de cas au limites")
	public boolean limite = false;

	@Option(type = OptionType.COMMAND, name = "-s", description = "Tests de la synthese")
	public boolean synthese = false;

	@Override
	public void run() {
		Client client = (Client) getIntance(clientClass);
		client.setHostPort(host, port);

		LOGGER.info("Implémentation de client : " + client.getClass().getCanonicalName());

		if (limite) {
			testLimites(client);
		}

		for (int i = 0; i < n; i++) {
			client.sendMessages(m, !synthese);
		}

		LOGGER.info((n * m) + " messages envoyés");

		if (synthese) {
			Map<Integer, Map> orig = getSynthese(client, getStart(), getDuration());
			for (int i = 0; i < 1000; i++) {
				Map<Integer, Map> nValue = getSynthese(client, getStart(), getDuration());
				if (!orig.equals(nValue)) {
					LOGGER.error("Différent!!! orig   : " + orig);
					LOGGER.error("Différent!!! nValue : " + nValue);
				}
				try {
					Thread.sleep(10L);
				} catch (InterruptedException e) {
					LOGGER.error("", e);
				}
			}
		}

		client.shutdown();
		System.exit(0);
	}

	protected int getDuration() {
		return 3600 * 2;
	}

	protected Date getStart() {
		Calendar start = Calendar.getInstance();
		start.add(Calendar.HOUR_OF_DAY, -1);
		return start.getTime();
	}

	protected Map<Integer, Map> getSynthese(Client client, Date start, int duration) {
		HashSet<Map> hashSet = new HashSet<>(JsonFactory.fromJson(client.getSynthese(start, duration), List.class));
		return hashSet.stream().collect(Collectors.toMap(e -> (Integer) e.get("sensorType"), e -> e, (k, v) -> {
			throw new RuntimeException(String.format("Duplicate key %s", k));
		}, TreeMap::new));
	}

	protected void testLimites(Client client) {
		LOGGER.info("Envoi de deux messages avec des très grandes valeurs");
		client.sendMessage(AbstractClient.getMessage(null, new Date(), 1, Long.MAX_VALUE - 100));
		client.sendMessage(AbstractClient.getMessage(null, new Date(), 1, Long.MAX_VALUE - 200));
		printSynthese(client, getStart(), getDuration());

		LOGGER.info("Envoi de deux messages avec des valeurs très éloignées");
		client.sendMessage(AbstractClient.getMessage(null, new Date(), -1, Long.MIN_VALUE));
		client.sendMessage(AbstractClient.getMessage(null, new Date(), -1, -1000L));
		client.sendMessage(AbstractClient.getMessage(null, new Date(), -1, 1L));
		client.sendMessage(AbstractClient.getMessage(null, new Date(), -1, 1000L));
		client.sendMessage(AbstractClient.getMessage(null, new Date(), -1, Long.MAX_VALUE));
		printSynthese(client, getStart(), getDuration());

		try {
			Thread.sleep(10000L);
		} catch (InterruptedException e1) {
			e1.printStackTrace();
		}

		printSynthese(client, getStart(), getDuration());

		LOGGER.info("Envoi du message regreergregregre");
		String message1 = AbstractClient.getMessage("regreergregregre", null, null, null);
		client.sendMessage(message1);

		String message = AbstractClient.getMessage(false);
		LOGGER.info("Envoi de 2 messages identiques");
		client.sendMessage(message);
		client.sendMessage(message);

	}

	protected void printSynthese(Client client, Date start, int duration) {
		LOGGER.info("Demande synthèse : ");
		String synthese = client.getSynthese(start, duration);
		LOGGER.info("Synthèse : " + synthese);
	}

	protected Object getIntance(String classe) {
		try {
			return Class.forName(classe).newInstance();
		} catch (Exception e) {
			throw new IllegalArgumentException(classe, e);
		}
	}

}
