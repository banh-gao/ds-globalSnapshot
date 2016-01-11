package it.unitn.ds;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Abstraction of a networked snapshot collector service
 * (Working only for branches running in the same JVM process)
 */
public class GlobalSnapshotCollector {

	private static int missingReports;
	private static long collectedBalance;

	private static CompletableFuture<Long> snapshotFut;

	private static final Map<Integer, PrintWriter> logFiles = new HashMap<Integer, PrintWriter>();

	public static void initSnapshot(int totalNodes) {
		missingReports = totalNodes;
		collectedBalance = 0;
		snapshotFut = new CompletableFuture<Long>();
	}

	/**
	 * Report the local branch snapshot to a snapshot collector service
	 */
	public static synchronized void reportLocalSnapshot(long snapshotId, int branchId, long balance, long incoming) {
		logLocal(snapshotId, branchId, balance, incoming);
		System.out.println(String.format("Reported snapshot %d:%d\ttotal %d\tbalance %d\tincoming %d", snapshotId, branchId, balance + incoming, balance, incoming));

		missingReports--;
		collectedBalance += balance + incoming;

		if (missingReports == 0)
			snapshotFut.complete(collectedBalance);
	}

	private static void logLocal(long snapshotId, int branchId, long balance, long incoming) {
		PrintWriter o = logFiles.computeIfAbsent(branchId, (id) -> {
			try {
				return new PrintWriter(new FileOutputStream(id + ".log"), true);
			} catch (FileNotFoundException e) {
				e.printStackTrace();
				return null;
			}
		});

		if (o != null)
			o.println(String.format("%d %d %d", snapshotId, balance, incoming));
	}

	public static CompletableFuture<Long> getGlobalBalance() {
		return snapshotFut;
	}
}
