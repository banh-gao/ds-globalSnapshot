package it.unitn.ds;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class Test {

	static int N_BRANCHES = 20;

	static long initialBalance = 10000;

	private static final Map<Integer, InetSocketAddress> branches = new HashMap<Integer, InetSocketAddress>(N_BRANCHES);

	public static void main(String[] args) throws Exception {

		// Generate branches -> internet sockets mapping
		for (int i = 0; i < N_BRANCHES; i++) {
			branches.put(i, new InetSocketAddress(2000 + i));
		}

		@SuppressWarnings("unchecked")
		CompletableFuture<Branch>[] b = new CompletableFuture[N_BRANCHES];

		branches.keySet().forEach(branchId -> {
			// Start all branches in parallel
			b[branchId] = Branch.start(branchId, branches, initialBalance);
		});

		// Wait until all branches are started
		CompletableFuture.allOf(b).join();

		// Keeps running the global snapshot algorithm
		int snapshotId = 0;
		while (true) {
			GlobalSnapshotCollector.initSnapshot(N_BRANCHES);

			// Always start snapshot from branch 0 (It can start everywhere but
			// concurrent snapshots are not supported by branches)
			b[0].get().startSnapshot(snapshotId);

			// Wait until the snapshot terminates globally
			long globalBalance = GlobalSnapshotCollector.getGlobalBalance().get();

			System.out.println("Snapshot " + snapshotId + " reports a global balance " + globalBalance + ", expected balance was " + N_BRANCHES * initialBalance);

			snapshotId = (snapshotId + 1) % Integer.MAX_VALUE;
		}
	}
}
