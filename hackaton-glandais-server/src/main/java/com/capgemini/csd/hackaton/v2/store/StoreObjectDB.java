package com.capgemini.csd.hackaton.v2.store;

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

import com.capgemini.csd.hackaton.v2.synthese.Summary;

public class StoreObjectDB implements Store {

	public final static Logger LOGGER = LoggerFactory.getLogger(StoreObjectDB.class);

	// stockage des messages
	private EntityManagerFactory entityManagerFactory;

	private EntityManager em;

	public void init(String dossier) {
		entityManagerFactory = Persistence.createEntityManagerFactory(dossier + "/messages.odb");
		em = entityManagerFactory.createEntityManager();
		em.getMetamodel().managedType(Message.class);
	}

	@Override
	public boolean containsId(String id) {
		return em.createNamedQuery("Message.exists", Long.class).setParameter("id", id).getSingleResult() > 0;
	}

	@Override
	public void indexMessages(List<Map<String, Object>> messages) {
		em.getTransaction().begin();
		for (Map<String, Object> map : messages) {
			Message message = new Message((String) map.get("id"), (Date) map.get("timestamp"),
					(Integer) map.get("sensorType"), (Long) map.get("value"));
			em.persist(message);
		}
		em.getTransaction().commit();
	}

	@Override
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
