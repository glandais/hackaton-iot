package com.capgemini.csd.hackaton.server;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.boon.IO;
import org.boon.json.JsonFactory;
import org.boon.json.ObjectMapper;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.NodeBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.LongTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.aggregations.metrics.avg.Avg;
import org.elasticsearch.search.aggregations.metrics.max.Max;
import org.elasticsearch.search.aggregations.metrics.min.Min;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.capgemini.csd.hackaton.v1.index.Index;

public class IndexElasticSearch implements Index {

	public final static Logger LOGGER = LoggerFactory.getLogger(IndexElasticSearch.class);

	private Client client;

	public IndexElasticSearch() {
		super();
	}

	@Override
	public void init(String dossier) {
		client = NodeBuilder.nodeBuilder().local(true)
				.settings(Settings.builder().put("http.enabled", "false").put("path.home", dossier)).node().client();
		String mapping = IO.read(IndexElasticSearch.class.getResourceAsStream("/message.json"));
		client.admin().indices().prepareCreate("iot")
				.setSettings(Settings.builder().put("number_of_shards", 1).put("number_of_replicas", 0))
				.addMapping("message", mapping).get();
	}

	@Override
	public boolean isInMemory() {
		return false;
	}

	@Override
	public void index(String json) throws Exception {
		Map<?, ?> map = JsonFactory.fromJson(json, Map.class);
		if (exists(map)) {
			throw new Exception("Element existant");
		} else {
			client.prepareIndex("iot", "message").setSource(json).execute().actionGet();
		}
		//		String json = jsonParam.replace("\"id\"", "\"_id\"");
		//		ListenableActionFuture<IndexResponse> resultat =
		//		client.prepareIndex("iot", "message").setSource(map).execute();
		//		try {
		//			LOGGER.info("" + resultat.get());
		//		} catch (InterruptedException | ExecutionException e) {
		//			LOGGER.error("Erreur", e);
		//		}
	}

	@Override
	public String getSynthese() {
		SearchRequestBuilder search = client.prepareSearch("iot").setTypes("message");
		search.setQuery(QueryBuilders.rangeQuery("timestamp").from("now-1h").to("now"));
		search.setSize(0);
		TermsBuilder groupBy = AggregationBuilders.terms("group_by").field("sensorType");
		groupBy.subAggregation(AggregationBuilders.avg("avg").field("value"));
		groupBy.subAggregation(AggregationBuilders.min("min").field("value"));
		groupBy.subAggregation(AggregationBuilders.max("max").field("value"));
		search.addAggregation(groupBy);
		SearchResponse result = search.execute().actionGet();
		LongTerms aggregation = result.getAggregations().get("group_by");
		List<Map<String, Object>> syntheses = new ArrayList<>();
		for (Bucket bucket : aggregation.getBuckets()) {
			Map<String, Object> synthesis = new HashMap<>();
			synthesis.put("sensorType", Integer.valueOf(bucket.getKeyAsString()));
			Avg avg = bucket.getAggregations().get("avg");
			Min min = bucket.getAggregations().get("min");
			Max max = bucket.getAggregations().get("max");
			synthesis.put("minValue", Math.round(min.getValue()));
			synthesis.put("maxValue", Math.round(max.getValue()));
			synthesis.put("mediumValue", Math.round(avg.getValue()));
			syntheses.add(synthesis);
		}
		return JsonFactory.toJson(syntheses);
	}

	public boolean exists(Map<?, ?> map) {
		String id = (String) map.get("id");
		SearchRequestBuilder search = client.prepareSearch("iot").setTypes("message");
		search.setQuery(QueryBuilders.termQuery("id", id));
		search.setSize(0);
		SearchResponse result = search.execute().actionGet();
		long totalHits = result.getHits().getTotalHits();
		return totalHits != 0;
	}

	@Override
	public void close() {
		client.close();
	}

	@Override
	public long getSize() {
		SearchRequestBuilder search = client.prepareSearch("iot").setTypes("message");
		search.setSize(0);
		SearchResponse result = search.execute().actionGet();
		return result.getHits().getTotalHits();
	}
}
