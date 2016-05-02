package com.capgemini.csd.hackaton.v2;

import com.capgemini.csd.hackaton.execution.ExecutionClient;

import io.airlift.airline.Cli;
import io.airlift.airline.Cli.CliBuilder;
import io.airlift.airline.Help;

public class Main {

	public static void main(String[] args) {
		CliBuilder<Runnable> builder = Cli.<Runnable> builder("hackaton-glandais").withDefaultCommand(Help.class)
				.withCommands(Help.class, IOTServerMapDB.class, IOTServerH2.class, IOTServerES.class,
						IOTServerODB.class, IOTServerH2ES.class, IOTServerH2Mem.class, IOTServerMem.class,
						IOTServerNoop.class, ExecutionClient.class);

		Cli<Runnable> parser = builder.build();
		parser.parse(args).run();
	}

}
