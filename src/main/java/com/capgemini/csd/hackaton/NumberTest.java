package com.capgemini.csd.hackaton;

public class NumberTest {

	public static void main(String[] args) {
		long v1 = 8234567890123456789L;
		long v2 = 8234567894123456789L;
		System.out.println((v1 + v2) / 2);
		double a1 = (v1 * 1.0 + v2 * 1.0) / 2.0;
		double ar = 8234567892123456789L;
		double aw = 8234567892123457000L;

		System.out.println(a1 == ar);
		System.out.println(a1 == aw);

		double a2 = Double.parseDouble(Double.toString(a1));
		System.out.println(a1 == a2);

	}
}
