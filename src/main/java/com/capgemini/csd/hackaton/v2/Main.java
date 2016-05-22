package com.capgemini.csd.hackaton.v2;

import com.capgemini.csd.hackaton.client.ExecutionClient;
import com.capgemini.csd.hackaton.v2.bench.ControlerBench;
import com.capgemini.csd.hackaton.v2.bench.ParseBench;
import com.capgemini.csd.hackaton.v2.bench.QueueBench;
import com.capgemini.csd.hackaton.v2.bench.StoreBench;
import com.capgemini.csd.hackaton.v3.IOTServerV3;

import io.airlift.airline.Cli;
import io.airlift.airline.Cli.CliBuilder;
import io.airlift.airline.Help;

public class Main {

	public static void main(String[] args) {
		CliBuilder<Runnable> builder = Cli.<Runnable> builder("hackaton-glandais").withDefaultCommand(Help.class)
				.withCommands(Help.class, IOTServerODB.class, IOTServerMem.class, IOTServerNoop.class,
						ExecutionClient.class, StoreBench.class, ParseBench.class, ControlerBench.class,
						QueueBench.class, IOTServerLucene.class, IOTServerMapDB.class, IOTServerMapDB2.class,
						IOTServerV3.class);

		Cli<Runnable> parser = builder.build();
		parser.parse(args).run();
	}

}
