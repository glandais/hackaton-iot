package org.hackaton.glandais.hackaton;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Store {

	public final static Logger LOGGER = LoggerFactory.getLogger(Store.class);

	// stockage des messages
	private EntityManagerFactory entityManagerFactory;

	private EntityManager em;

	public void init(String dossier) {
		entityManagerFactory = Persistence.createEntityManagerFactory(dossier + "/messages.odb");
		em = entityManagerFactory.createEntityManager();
		em.getMetamodel().managedType(Message.class);
	}

	public boolean containsId(String id) {
		return em.createNamedQuery("Message.exists", Long.class).setParameter("id", id).getSingleResult() > 0;
	}

	public void indexMessages(List<Message> messages) {
		em.getTransaction().begin();
		for (Message message : messages) {
			em.persist(message);
		}
		em.getTransaction().commit();
	}

	public Map<Integer, Summary> getSummary(long timestamp, Integer duration) {
		Map<Integer, Summary> res = new HashMap<>();
		List<Object[]> summaries = em.createNamedQuery("Message.summary").setParameter("start", new Date(timestamp))
				.setParameter("end", new Date(timestamp + 1000 * duration)).getResultList();
		res = summaries.stream()
				.collect(Collectors.toMap(a -> (Integer) a[0], a -> new Summary(((Number) a[0]).intValue(),
						(Number) a[1], (Number) a[2], (Number) a[3], (Number) a[4])));
		return res;
	}

}
