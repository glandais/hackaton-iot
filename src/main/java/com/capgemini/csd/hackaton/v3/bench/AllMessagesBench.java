package com.capgemini.csd.hackaton.v3.bench;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.io.FileUtils;
import org.apache.lucene.analysis.core.StopAnalyzer;

import com.capgemini.csd.hackaton.v3.messages.AllMessagesCustom;
import com.capgemini.csd.hackaton.v3.messages.AllMessagesGuava;
import com.capgemini.csd.hackaton.v3.messages.IAllMessages;
import com.google.common.base.Stopwatch;

public class AllMessagesBench {

	public static void main(String[] args) {
		bench(new AllMessagesCustom());
		bench(new AllMessagesGuava());
		bench(new AllMessagesCustom());
		bench(new AllMessagesGuava());
	}

	private static void bench(IAllMessages allMessages) {
		String tmpDossier = getTmpDossier();
		new File(tmpDossier).mkdirs();
		allMessages.init(tmpDossier);

		ExecutorService executor = Executors.newFixedThreadPool(4);
		List<Future> futures = new ArrayList<>();
		Stopwatch sw = null;
		for (int i = -20000; i < 1000000; i++) {
			if (i == 0) {
				sw = Stopwatch.createStarted();
			}
			int s = i % 100;
			boolean b = (i % 2 == 0) ? true : false;
			futures.add(executor.submit(() -> {
				if (b) {
					allMessages.getForWrite(s);
				} else {
					allMessages.getForRead(s);
				}
			}));
		}
		for (Future future : futures) {
			try {
				future.get();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ExecutionException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		System.out.println(allMessages.getClass().getName() + " " + sw);

		allMessages.close();
		try {
			FileUtils.deleteDirectory(new File(tmpDossier));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static String getTmpDossier() {
		try {
			File tmpFile = File.createTempFile("bench", "store");
			tmpFile.delete();
			tmpFile.mkdirs();
			return tmpFile.getAbsolutePath();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

}
