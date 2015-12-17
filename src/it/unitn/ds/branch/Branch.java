package it.unitn.ds.branch;

import it.unitn.ds.net.NetOverlay;
import it.unitn.ds.net.NetOverlay.Message;
import it.unitn.ds.net.NetOverlay.Token;
import it.unitn.ds.net.NetOverlay.Transfer;
import it.unitn.ds.net.UDPNetOverlay;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

public class Branch {

	private static final int MAX_TRANSFER = 100;

	private final ExecutorService BRANCH_THREADS = Executors.newSingleThreadExecutor(new ThreadFactory() {

		@Override
		public Thread newThread(Runnable r) {
			Thread t = new Thread(r, "Branch Thread");
			t.setDaemon(true);
			return t;
		}
	});

	private final int localId;
	private final Map<Integer, InetSocketAddress> branches;

	private final NetOverlay overlay;

	private long balance;
	private boolean stop = false;

	private List<Integer> randBranches;
	private int nextBranch = 0;
	private Random rand = new Random();

	public Branch(int localId, Map<Integer, InetSocketAddress> branches, long initialBalance) {
		this.localId = localId;
		this.branches = branches;

		overlay = new UDPNetOverlay();

		this.balance = initialBalance;

		randBranches = new ArrayList<Integer>(branches.keySet());
		randBranches.remove(localId);
		Collections.shuffle(randBranches);
	}

	public void start() throws IOException, InterruptedException {
		overlay.start(localId, branches);
		startExecutionLoop();
	}

	private void startExecutionLoop() {
		BRANCH_THREADS.execute(new Runnable() {

			@Override
			public void run() {
				while (stop == false) {
					try {
						Message m = overlay.receiveMessage();
						if (m != null)
							processMessage(m);
					} catch (Exception e) {
						e.printStackTrace();
					}

					// FIXME: balance reduction not executed by executor
					// Once the transfer is completed the local balance is
					// reduced by the same thread executor
					sendRandomTransfer().thenAcceptAsync((m) -> {
						System.out.println("COMPLETE");
						balance -= ((Transfer) m).getAmount();
					}, BRANCH_THREADS);

					try {
						Thread.sleep(200);
					} catch (InterruptedException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
					System.out.println("CURRENT BALANCE in " + localId + ": " + balance);
				}
			}
		});
	}

	private void processMessage(Message m) {
		if (m instanceof Transfer)
			processTransfer((Transfer) m);
		else
			processToken((Token) m);
	}

	private void processTransfer(Transfer m) {
		// Increase balance
		balance += ((Transfer) m).getAmount();
		// TODO Auto-generated method stub

	}

	private void processToken(Token m) {
		// TODO Auto-generated method stub
	}

	private CompletableFuture<Message> sendRandomTransfer() {
		if (balance == 0)
			return CompletableFuture.completedFuture(new Transfer(0));

		int maxValue = (int) Math.min(MAX_TRANSFER, balance);
		long amount = (maxValue > 0) ? 1 + (rand.nextInt(maxValue)) : 1;
		return overlay.sendMessage(getRandomBranch(), new Transfer(amount));
	}

	// Randomly choose a destination branch
	private int getRandomBranch() {
		if (this.nextBranch == randBranches.size()) {
			Collections.shuffle(randBranches);
			this.nextBranch = 0;
		}
		return randBranches.get(this.nextBranch++);

	}

	public void stop() {
		stop = true;
	}
}
