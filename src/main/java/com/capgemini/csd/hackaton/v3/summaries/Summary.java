package com.capgemini.csd.hackaton.v3.summaries;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.RoundingMode;

public class Summary implements Serializable {

	private static final long serialVersionUID = -3701812119259762447L;

	private int sensorType;
	private Long min = null;
	private Long max = null;
	private BigDecimal total = BigDecimal.valueOf(0);
	private long count = 0;

	public Summary() {
		super();
	}

	public Summary(int sensorType) {
		super();
		this.sensorType = sensorType;
	}

	public Summary(Summary summary) {
		this(summary.sensorType, summary.count, summary.total, summary.min, summary.max);
	}

	public Summary(int sensorType, Number count, Number total, Number min, Number max) {
		this.sensorType = sensorType;
		this.min = min.longValue();
		this.max = max.longValue();
		this.count = count.longValue();
		if (total instanceof BigDecimal) {
			this.total = (BigDecimal) total;
		} else {
			this.total = BigDecimal.valueOf(total.doubleValue());
		}
	}

	public int getSensorType() {
		return sensorType;
	}

	public Long getMin() {
		return min;
	}

	public Long getMax() {
		return max;
	}

	public BigDecimal getAverage() {
		return total.divide(BigDecimal.valueOf(count), 2, RoundingMode.HALF_UP);
	}

	public void setSensorType(int sensorType) {
		this.sensorType = sensorType;
	}

	public synchronized void accept(long i) {
		min = min(min, i);
		max = max(max, i);
		total = total.add(BigDecimal.valueOf(i));
		count++;
	}

	public void combine(Summary other) {
		min = min(min, other.min);
		max = max(max, other.max);
		total = total.add(other.total);
		count = count + other.count;
	}

	public static Summary combine2(Summary other1, Summary other2) {
		Summary res = new Summary();
		res.sensorType = other1.sensorType;
		res.min = min(other1.min, other2.min);
		res.max = max(other1.max, other2.max);
		res.total = other1.total.add(other2.total);
		res.count = other1.count + other2.count;
		return res;
	}

	private static Long max(Long max1, Long max2) {
		if (max1 == null) {
			return max2;
		} else if (max2 == null) {
			return max1;
		} else {
			return Math.max(max1, max2);
		}
	}

	private static Long min(Long min1, Long min2) {
		if (min1 == null) {
			return min2;
		} else if (min2 == null) {
			return min1;
		} else {
			return Math.min(min1, min2);
		}
	}

	@Override
	public String toString() {
		BigDecimal average = total.divide(BigDecimal.valueOf(count), 2, RoundingMode.HALF_UP);
		return "{\"sensorType\":" + sensorType + ",\"minValue\":" + min + ",\"maxValue\":" + max + ",\"mediumValue\":"
				+ average.toString() + ",\"count\":" + count + "}";
	}

}
