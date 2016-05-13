package com.capgemini.csd.hackaton;

public interface Controler {

	String processRequest(String uri, String message) throws Exception;

	void setDossier(String dossier);

	String getDossier();

	void configure();

	void close();

}
