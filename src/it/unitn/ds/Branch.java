package it.unitn.ds;

import it.unitn.ds.net.NetOverlay;
import it.unitn.ds.net.NetOverlay.Message;
import it.unitn.ds.net.NetOverlay.Token;
import it.unitn.ds.net.NetOverlay.Transfer;
import it.unitn.ds.net.UDPNetOverlay;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;

/**
 * Branch implementation that performs the following actions: - Incoming
 * transfer processing - Random transfer transmission - Global snapshot support
 * and local state reporting to a collector service
 */
public class Branch {

	/**
	 * Initial balance for branch
	 */
	public static final long INITIAL_BALANCE = 1000000;

	/**
	 * Maximum amount of money for a single transfer
	 */
	public static final int MAX_TRANSFER = 100;

	private final ScheduledExecutorService MAIN_LOOP = Executors
			.newSingleThreadScheduledExecutor(new ThreadFactory() {

				@Override
				public Thread newThread(Runnable r) {
					Thread t = new Thread(r, "Branch Looper");
					t.setDaemon(true);
					return t;
				}
			});

	private final int localId;
	private final Map<Integer, InetSocketAddress> branches;

	private final NetOverlay overlay = new UDPNetOverlay();
	private final SnapshotHelper snapshot = new SnapshotHelper();
	private final Random rand = new Random();

	// Branches random selection
	private List<Integer> randBranches;
	private int nextBranch = 0;

	// Money available (not reserved) that can be used for transfers
	private long availableAmounts = INITIAL_BALANCE;

	// Money reserved for pending outgoing transfers
	private long reservedAmounts = 0;

	/**
	 * Start a new branch
	 * 
	 * @param localId
	 * @param branches
	 * @param initialBalance
	 * @return A future is completed once the branch is started
	 */
	public static CompletableFuture<Branch> start(int localId,
			Map<Integer, InetSocketAddress> branches) {

		Branch b = new Branch(localId, branches);

		return b.overlay.start(localId, branches).thenApply(b::startActivity);
	}

	private Branch(int localId, Map<Integer, InetSocketAddress> branches) {
		this.localId = localId;
		this.branches = branches;

		// Initialize random branches selection
		randBranches = new ArrayList<Integer>(branches.keySet());
		randBranches.remove(localId);
		Collections.shuffle(randBranches);
	}

	private Branch startActivity(Void v) {
		// If only one branch, messaging not active
		if (branches.size() == 1)
			return this;

		// Triggers message processing once the first message arrives
		overlay.receiveMessage().thenAcceptAsync(
				inMsg -> processMessage(inMsg), MAIN_LOOP);

		// Start random money transfers transmission
		MAIN_LOOP.execute(this::sendRandomTransfer);
		return this;
	}

	private void processMessage(Message m) {

		if (m instanceof Transfer)
			processTransfer((Transfer) m);
		else
			processToken((Token) m);

		// Reschedule for receiving the next message
		overlay.receiveMessage().thenAcceptAsync(
				inMsg -> processMessage(inMsg), MAIN_LOOP);
	}

	private void processTransfer(Transfer m) {
		// If snapshot is active the transfer may be counted
		snapshot.newTransferReceived(m.getSenderId(), m.getAmount());

		// Increase balance
		availableAmounts += m.getAmount();
	}

	private void processToken(Token m) {
		boolean isNewSnapshot = snapshot.newTokenReceived(m.getSenderId(),
				m.getSnapshotId(), availableAmounts);

		if (!isNewSnapshot)
			return;

		// Broadcasts tokens to all branches except itself
		try {
			broadcastTokens(m.getSnapshotId());
		} catch (InterruptedException | ExecutionException e) {
			e.printStackTrace();
		}
	}

	// Transfer random money to random branch
	private void sendRandomTransfer() {

		if (availableAmounts == 0)
			return;

		int maxValue = (int) Math.min(MAX_TRANSFER, availableAmounts);
		long amount = (maxValue > 0) ? 1 + (rand.nextInt(maxValue)) : 1;

		// Move the transferred amount from available to reserved
		availableAmounts -= amount;
		reservedAmounts += amount;

		// Once the transfer is completed the branch thread:
		// 1. reduces the reserved amounts
		// 2. start a new random transfer
		CompletableFuture<Void> outcome = overlay.sendMessage(
				getRandomBranch(), new Transfer(amount)).thenAcceptAsync(t -> {
			reservedAmounts -= t.getAmount();
		}, MAIN_LOOP);

		// Delivery successful: schedule next transfer
		outcome.thenRunAsync(this::sendRandomTransfer, MAIN_LOOP);

		// Delivery failed: restore transfer amounts and schedule next transfer
		outcome.exceptionally((ex) -> {
			MAIN_LOOP.execute(() -> {
				availableAmounts += amount;
				reservedAmounts -= amount;
				sendRandomTransfer();
			});
			return null;
		});
	}

	// Randomly choose a destination branch
	private int getRandomBranch() {
		if (this.nextBranch == randBranches.size()) {
			Collections.shuffle(randBranches);
			this.nextBranch = 0;
		}
		return randBranches.get(this.nextBranch++);
	}

	public CompletableFuture<Long> startSnapshot(int snapshotId) {
		CompletableFuture<Long> snapFut = new CompletableFuture<Long>();

		System.out.println("Starting global snapshot " + snapshotId
				+ " from branch " + localId);

		MAIN_LOOP.execute(() -> {
			snapshot.startSnapshot(snapshotId, availableAmounts).thenAccept(
					(v) -> snapFut.complete(v));

			// Send tokens to all other branches, no transfer occurs between
			// saving the availableAmounts and queuing tokens for broadcast
				try {
					broadcastTokens(snapshotId);
				} catch (InterruptedException | ExecutionException e) {
					e.printStackTrace();
				}
			});
		return snapFut;
	}

	private void broadcastTokens(long snapshotId) throws InterruptedException,
			ExecutionException {
		Set<Integer> destBranches = new HashSet<Integer>(branches.keySet());

		destBranches.remove(localId);

		for (int branch : destBranches) {
			overlay.sendMessage(branch, new Token(snapshotId));
		}
	}

	/**
	 * @return The total branch balance: available amounts + amounts reserved
	 *         for transfers
	 */
	public long getTotalBalance() {
		return availableAmounts + reservedAmounts;
	}

	/**
	 * @return The balance available to the branch
	 */
	public long getAvailableBalance() {
		return availableAmounts;
	}

	public void stop() {
		MAIN_LOOP.shutdown();
	}

	/**
	 * Helper class to manage the snapshot state for the local branch
	 */
	class SnapshotHelper {

		private final Set<Integer> receivedTokens = new HashSet<Integer>();

		private CompletableFuture<Long> snapFut;
		private boolean isSnapshotMode = false;
		private long snapshotId;
		private long branchBalance;
		private long incomingTransfers;

		/**
		 * 
		 * @param branch
		 * @param snapshotId
		 * @param currentBalance
		 * @return Returns true if the token has triggered a new snapshot in the
		 *         local node
		 */
		public boolean newTokenReceived(int branch, long snapshotId,
				long currentBalance) {
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
			if (receivedTokens.size() == branches.size())
				stopSnapshot();

			return newSnapshotStarted;
		}

		public void newTransferReceived(int branch, long amount) {
			if (!isSnapshotMode)
				return;

			// If token was not received the transfer has to be counted in
			// current
			// snapshot
			if (!receivedTokens.contains(branch)) {
				incomingTransfers += amount;
			}
		}

		public CompletableFuture<Long> startSnapshot(long snapshotId,
				long currentBalance) {
			snapFut = new CompletableFuture<Long>();
			if (snapshot.isActive()) {
				snapFut.completeExceptionally(new IllegalStateException(
						"Skipped global snapshot " + snapshotId
								+ " from branch " + localId + " (snapshot "
								+ snapshot.getActiveId()
								+ " still in progress)"));
				return snapFut;
			}

			isSnapshotMode = true;
			this.snapshotId = snapshotId;
			incomingTransfers = 0;

			// Save local snapshot status
			branchBalance = currentBalance;
			receivedTokens.add(localId);

			// Only one branch, local snapshot terminates immediately
			if (branches.size() == 1)
				stopSnapshot();

			return snapFut;
		}

		private void stopSnapshot() {
			isSnapshotMode = false;
			GlobalSnapshotCollector.reportLocalSnapshot(snapshotId, localId,
					branchBalance, incomingTransfers);

			snapFut.complete(branchBalance + incomingTransfers);
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
}
