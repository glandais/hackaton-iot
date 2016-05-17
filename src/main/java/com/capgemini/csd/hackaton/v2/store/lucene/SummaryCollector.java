package com.capgemini.csd.hackaton.v2.store.lucene;

import java.io.IOException;
import java.util.Map;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.FunctionValues.ValueFiller;
import org.apache.lucene.queries.function.valuesource.IntFieldSource;
import org.apache.lucene.queries.function.valuesource.LongFieldSource;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.SimpleCollector;
import org.apache.lucene.util.mutable.MutableValue;
import org.apache.lucene.util.mutable.MutableValueInt;
import org.apache.lucene.util.mutable.MutableValueLong;

import com.capgemini.csd.hackaton.client.Summary;

import net.openhft.koloboke.collect.map.hash.HashIntObjMaps;

public class SummaryCollector extends SimpleCollector {

	private Map<Integer, Summary> summaries;

	private FunctionValues sensorTypefunction;

	private ValueFiller sensorTypeValueFiller;

	private MutableValue sensorTypeValue;

	private FunctionValues valueFunction;

	private ValueFiller valueValueFiller;

	private MutableValue valueValue;

	private static final IntFieldSource SENSOR_TYPE = new IntFieldSource("sensorType");

	private static final LongFieldSource VALUE = new LongFieldSource("value");

	public SummaryCollector() {
		super();
		this.summaries = HashIntObjMaps.newMutableMap();
	}

	@Override
	public boolean needsScores() {
		return false;
	}

	@Override
	public void setScorer(Scorer scorer) throws IOException {
		// noop
	}

	@Override
	protected void doSetNextReader(LeafReaderContext context) throws IOException {
		sensorTypefunction = SENSOR_TYPE.getValues(null, context);
		sensorTypeValueFiller = sensorTypefunction.getValueFiller();
		sensorTypeValue = sensorTypeValueFiller.getValue();

		valueFunction = VALUE.getValues(null, context);
		valueValueFiller = valueFunction.getValueFiller();
		valueValue = valueValueFiller.getValue();
	}

	@Override
	public void collect(int doc) throws IOException {
		sensorTypeValueFiller.fillValue(doc);
		valueValueFiller.fillValue(doc);
		if (sensorTypeValue.exists && valueValue.exists) {
			int sensorType = ((MutableValueInt) sensorTypeValue).value;
			long value = ((MutableValueLong) valueValue).value;
			Summary summary = summaries.get(sensorType);
			if (summary == null) {
				summary = new Summary(sensorType);
				summaries.put(sensorType, summary);
			}
			summary.accept(value);
		}
	}

	public Map<Integer, Summary> getSummaries() {
		return summaries;
	}

}
