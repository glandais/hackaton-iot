package com.capgemini.csd.hackaton.v1;

import com.capgemini.csd.hackaton.client.ExecutionClient;
import com.capgemini.csd.hackaton.v1.bench.BenchIndex;
import com.capgemini.csd.hackaton.v1.bench.BenchQueue;
import com.capgemini.csd.hackaton.v1.execution.ExecutionServer;

import io.airlift.airline.Cli;
import io.airlift.airline.Cli.CliBuilder;
import io.airlift.airline.Help;

public class Main {

	public static void main(String[] args) {
		CliBuilder<Runnable> builder = Cli.<Runnable> builder("hackaton-glandais").withDefaultCommand(Help.class)
				.withCommands(Help.class);

		builder.withGroup("benchmark").withDescription("Benchs des composants").withDefaultCommand(BenchIndex.class)
				.withCommands(BenchIndex.class, BenchQueue.class);

		builder.withGroup("run").withDescription("Exécution").withDefaultCommand(ExecutionServer.class)
				.withCommands(ExecutionServer.class, ExecutionClient.class);

		Cli<Runnable> parser = builder.build();
		parser.parse(args).run();
	}

}
