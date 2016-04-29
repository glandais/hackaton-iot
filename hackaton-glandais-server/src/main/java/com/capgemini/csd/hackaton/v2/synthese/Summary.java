package com.capgemini.csd.hackaton.v2.synthese;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.HashMap;
import java.util.Map;
import java.util.function.LongConsumer;

public class Summary implements LongConsumer {

	private int sensorType;
	private Long min = null;
	private Long max = null;
	private BigDecimal total = BigDecimal.valueOf(0);
	private long count = 0;

	public Summary(int sensorType) {
		super();
		this.sensorType = sensorType;
	}

	public Summary(int sensorType, Number count, Number total, Number min, Number max) {
		this(sensorType);
		this.min = min.longValue();
		this.max = max.longValue();
		this.count = count.longValue();
		if (total instanceof BigDecimal) {
			this.total = (BigDecimal) total;
		} else {
			this.total = BigDecimal.valueOf(total.doubleValue());
		}
	}

	public void accept(long i) {
		min = min(min, i);
		max = max(max, i);
		total = total.add(BigDecimal.valueOf(i));
		count++;
	}

	public void combine(Summary other) {
		min = min(min, other.min);
		max = max(max, other.max);
		total = total.add(other.total);
		count += other.count;
	}

	private Long max(Long max1, Long max2) {
		if (max1 == null) {
			return max2;
		} else if (max2 == null) {
			return max1;
		} else {
			return Math.max(max1, max2);
		}
	}

	private Long min(Long min1, Long min2) {
		if (min1 == null) {
			return min2;
		} else if (min2 == null) {
			return min1;
		} else {
			return Math.min(min1, min2);
		}
	}

	public Map<String, Object> toMap() {
		HashMap<String, Object> map = new HashMap<>();
		map.put("sensorType", sensorType);
		map.put("minValue", min);
		map.put("maxValue", max);
		BigDecimal average = total.divide(BigDecimal.valueOf(count), 2, RoundingMode.HALF_DOWN);
		map.put("mediumValue", average);
		return map;
	}

	@Override
	public String toString() {
		return toMap().toString();
	}
}
