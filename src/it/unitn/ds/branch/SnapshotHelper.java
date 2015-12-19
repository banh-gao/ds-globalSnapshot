package it.unitn.ds.branch;

import java.util.HashSet;
import java.util.Set;

class SnapshotHelper {

	private final Set<Integer> receivedTokens;
	private final int totalBranches;
	private final int localBranchId;

	private boolean isSnapshotMode = false;
	private long snapshotId;
	private long branchBalance;
	private long incomingTransfers;

	public SnapshotHelper(int totalBranches, int localBranchId) {
		this.totalBranches = totalBranches;
		receivedTokens = new HashSet<Integer>(totalBranches);
		this.localBranchId = localBranchId;
	}

	/**
	 * 
	 * @param branch
	 * @param snapshotId
	 * @param currentBalance
	 * @return Returns true if the token has triggered a new snapshot in the
	 *         local node
	 */
	public boolean newTokenReceived(int branch, long snapshotId, long currentBalance) {
		// Discard token not matching current snapshot id
		if (isSnapshotMode && snapshotId != this.snapshotId) {
			System.out.println("Token not matching active snapshot ID");
			return false;
		}

		boolean newSnapshotStarted = false;

		// If snapshot mode is not active, this token triggers a new one
		if (!isSnapshotMode) {
			startSnapshot(snapshotId, currentBalance);
			newSnapshotStarted = true;
		}

		// Try to add a token
		if (!receivedTokens.add(branch))
			System.out.println("Token already received!");

		// Token received from all branches, local snapshot is terminated
		if (receivedTokens.size() == totalBranches)
			stopSnapshot();

		return newSnapshotStarted;
	}

	public void newTransferReceived(int branch, long amount) {
		if (!isSnapshotMode)
			return;

		// If token was not received the transfer has to be counted in current
		// snapshot
		if (!receivedTokens.contains(branch)) {
			incomingTransfers += amount;
		}
	}

	public void startSnapshot(long snapshotId, long currentBalance) {
		isSnapshotMode = true;
		this.snapshotId = snapshotId;
		incomingTransfers = 0;

		// Save local snapshot status
		branchBalance = currentBalance;
		receivedTokens.add(localBranchId);
	}

	private void stopSnapshot() {
		isSnapshotMode = false;
		GlobalSnapshotCollector.saveLocalSnapshot(snapshotId, localBranchId, branchBalance, incomingTransfers);

		receivedTokens.clear();
	}

	public boolean isTokenArrived(int branch) {
		return receivedTokens.contains(branch);
	}

	public boolean isActive() {
		return isSnapshotMode;
	}

	public long getActiveId() {
		return snapshotId;
	}
}
