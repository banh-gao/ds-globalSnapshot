package it.unitn.ds;
import it.unitn.ds.branch.Branch;
import it.unitn.ds.branch.GlobalSnapshotCollector;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

public class Test {

	static int N_BRANCHES = 100;

	static long balance = 10000;

	private static final Map<Integer, InetSocketAddress> branches = new HashMap<Integer, InetSocketAddress>(N_BRANCHES);

	public static void main(String[] args) throws Exception {

		// Generate branches -> internet sockets mapping
		for (int i = 0; i < N_BRANCHES; i++) {
			branches.put(i, new InetSocketAddress(2000 + i));
		}

		// Start all branches, they send random money transfers to each other
		Branch[] b = new Branch[N_BRANCHES];
		for (int i = 0; i < N_BRANCHES; i++) {
			b[i] = new Branch(i, branches, balance);
			b[i].start();
		}

		// Periodically run the global snapshot algorithm
		int snapshotId = 0;
		while (true) {
			GlobalSnapshotCollector.initSnapshot(N_BRANCHES);
			b[0].startSnapshot(snapshotId);
			snapshotId = (snapshotId + 1) % Integer.MAX_VALUE;

			// Busy wait until the snapshot terminates
			while (GlobalSnapshotCollector.isSnapshotActive())
				Thread.sleep(1000);

			System.out.println("Snapshot balance is " + GlobalSnapshotCollector.getTotalBalance() + ", expected balance was " + N_BRANCHES * balance);
		}
	}
}
