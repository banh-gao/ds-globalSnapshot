package it.unitn.ds.net;

import it.unitn.ds.net.NetOverlay.Message;
import it.unitn.ds.net.NetOverlay.Transfer;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

public class Test {

	static int N_BRANCHES = 100;

	static long balance;

	private static final Map<Integer, InetSocketAddress> branches = new HashMap<Integer, InetSocketAddress>(N_BRANCHES);

	public static void main(String[] args) throws Exception {

		for (int i = 0; i < N_BRANCHES; i++) {
			branches.put(i, new InetSocketAddress(2000 + i));
		}

		UDPNetOverlay[] ov = new UDPNetOverlay[N_BRANCHES];

		for (int i = 0; i < N_BRANCHES; i++) {
			ov[i] = new UDPNetOverlay();
			ov[i].start(i, branches);
		}

		for (int i = 1; i < N_BRANCHES; i++) {
			CompletableFuture<Message> o = ov[i].sendMessage(0, new NetOverlay.Transfer(100));
			CompletableFuture<Long> transfer = o.thenApply(reduceBalance);
			System.out.println("NEW BALANCE: " + transfer.get());
		}

		for (int i = 1; i < N_BRANCHES; i++) {
			Message m = ov[0].receiveMessage();
			while (m == null)
				m = ov[0].receiveMessage();

			System.out.println("RECEIVED " + m);
		}
	}

	static Function<Message, Long> reduceBalance = (Message m) -> {
		long newBalance = Test.balance - ((Transfer) m).getAmount();
		return newBalance;
	};
}
