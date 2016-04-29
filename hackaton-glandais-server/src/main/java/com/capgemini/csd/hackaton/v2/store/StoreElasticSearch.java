package com.capgemini.csd.hackaton.v2.store;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.boon.IO;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
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
import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregation.SingleValue;
import org.elasticsearch.search.aggregations.metrics.max.Max;
import org.elasticsearch.search.aggregations.metrics.min.Min;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.capgemini.csd.hackaton.v1.index.IndexElasticSearch;
import com.capgemini.csd.hackaton.v2.synthese.Summary;

public class StoreElasticSearch implements Store {

	public final static Logger LOGGER = LoggerFactory.getLogger(StoreElasticSearch.class);

	private Client client;

	public void init(String dossier) {
		client = NodeBuilder.nodeBuilder().local(true).settings(Settings.builder().put("path.home", dossier)).node()
				.client();
		boolean exists = client.admin().indices().exists(new IndicesExistsRequest("iot")).actionGet().isExists();
		if (!exists) {
			String mapping = IO.read(IndexElasticSearch.class.getResourceAsStream("/message.json"));
			client.admin().indices().prepareCreate("iot")
					.setSettings(Settings.builder().put("number_of_shards", 1).put("number_of_replicas", 0))
					.addMapping("message", mapping).get();
		}
	}

	@Override
	public Map<Integer, Summary> getSummary() {
		SearchRequestBuilder search = client.prepareSearch("iot").setTypes("message");
		search.setQuery(QueryBuilders.rangeQuery("timestamp").from("now-1h").to("now"));
		search.setSize(0);
		TermsBuilder groupBy = AggregationBuilders.terms("group_by").field("sensorType");
		groupBy.subAggregation(AggregationBuilders.count("count").field("value"));
		groupBy.subAggregation(AggregationBuilders.sum("sum").field("value"));
		groupBy.subAggregation(AggregationBuilders.min("min").field("value"));
		groupBy.subAggregation(AggregationBuilders.max("max").field("value"));
		search.addAggregation(groupBy);
		SearchResponse result = search.execute().actionGet();
		LongTerms aggregation = result.getAggregations().get("group_by");
		Map<Integer, Summary> syntheses = new TreeMap<>();
		for (Bucket bucket : aggregation.getBuckets()) {
			bucket.getAggregations().get("count");
			SingleValue sum = bucket.getAggregations().get("sum");
			SingleValue count = bucket.getAggregations().get("count");
			Min min = bucket.getAggregations().get("min");
			Max max = bucket.getAggregations().get("max");
			Integer sensorType = Integer.valueOf(bucket.getKeyAsString());
			Summary synthesis = new Summary(sensorType, count.value(), sum.value(), min.value(), max.value());
			syntheses.put(sensorType, synthesis);
		}
		return syntheses;
	}

	@Override
	public boolean containsId(String id) {
		SearchRequestBuilder search = client.prepareSearch("iot").setTypes("message");
		search.setQuery(QueryBuilders.termQuery("id", id));
		search.setSize(0);
		SearchResponse result = search.execute().actionGet();
		long totalHits = result.getHits().getTotalHits();
		return totalHits != 0;
	}

	@Override
	public void indexMessages(List<Map<String, Object>> messages) {
		BulkRequestBuilder bulk = client.prepareBulk();
		for (Map<String, Object> map : messages) {
			bulk.add(client.prepareIndex("iot", "message").setSource(map));
		}
		bulk.execute().actionGet();
	}

}
