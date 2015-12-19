package it.unitn.ds.branch;

/**
 * Abstraction of a networked snapshot collector service
 */
public class GlobalSnapshotCollector {

	private static boolean isSnapshotActive = false;
	private static int missingReports;
	private static long totalBalance;

	public static void initSnapshot(int totalNodes) {
		isSnapshotActive = true;
		missingReports = totalNodes;
		totalBalance = 0;
	}

	public static boolean isSnapshotActive() {
		return isSnapshotActive;
	}

	/**
	 * Save the local branch snapshot to a snapshot collector service
	 */
	public static synchronized void saveLocalSnapshot(long snapshotId, int branchId, long balance, long incoming) {
		System.out.println(String.format("Snap %d:%d | bal %d | inc %d | total %d", snapshotId, branchId, balance, incoming, balance + incoming));

		missingReports--;
		if (missingReports == 0)
			isSnapshotActive = false;
		totalBalance += balance + incoming;
	}

	public static long getTotalBalance() {
		return totalBalance;
	}
}
