package com.capgemini.csd.hackaton;

import java.util.Collection;
import java.util.Map;

public interface Controler {

	String processRequest(String uri, Map<String, ? extends Collection<String>> params, String message)
			throws Exception;

	void setDossier(String dossier);

	String getDossier();

	void configure();

	void close();

	int getPort();

	void setPort(int i);

	void startServer(boolean b);

	long getQueueSize();

}
