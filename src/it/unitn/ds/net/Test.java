package it.unitn.ds.net;

import it.unitn.ds.net.NetOverlay.Message;
import it.unitn.ds.net.NetOverlay.Transfer;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.function.Function;

public class Test {

	static int N_BRANCHES = 100;

	static long balance = 10000;

	private static final Map<Integer, InetSocketAddress> branches = new HashMap<Integer, InetSocketAddress>(N_BRANCHES);

	private static final ExecutorService BRANCH_POOL = Executors.newSingleThreadExecutor(new ThreadFactory() {

		@Override
		public Thread newThread(Runnable r) {
			Thread t = new Thread(r, "Branch Thread");
			t.setDaemon(true);
			return t;
		}
	});

	public static void main(String[] args) throws Exception {

		for (int i = 0; i < N_BRANCHES; i++) {
			branches.put(i, new InetSocketAddress(2000 + i));
		}

		UDPNetOverlay[] ov = new UDPNetOverlay[N_BRANCHES];

		for (int i = 0; i < N_BRANCHES; i++) {
			ov[i] = new UDPNetOverlay();
			ov[i].start(i, branches, BRANCH_POOL);
		}

		for (int i = 1; i < N_BRANCHES; i++) {
			CompletableFuture<Message> o = ov[i].sendMessage(0, new NetOverlay.Transfer(100));
			CompletableFuture<Long> transfer = o.thenApply(reduceBalance);
			System.out.println("NEW BALANCE: " + transfer.get());

		}

		for (int i = 1; i < N_BRANCHES; i++) {
			Message m = ov[0].receiveMessage().get();
			while (m == null) {
				m = ov[0].receiveMessage().get();
			}
			System.out.println("RECEIVED " + m);

		}

	}

	static Function<Message, Long> reduceBalance = (Message m) -> {
		Test.balance = Test.balance - ((Transfer) m).getAmount();
		return Test.balance;
	};
}
