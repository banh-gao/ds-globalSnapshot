package it.unitn.ds.net;

import it.unitn.ds.net.NetOverlay.Message;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class Test {

	static int N_BRANCHES = 100;

	private static final ExecutorService exec = Executors.newFixedThreadPool(N_BRANCHES);

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
			exec.execute(new Branch(ov[i]));
		}

		for (int i = 1; i < N_BRANCHES; i++) {
			Message m = ov[0].receiveMessage();
			while (m == null)
				m = ov[0].receiveMessage();

			System.out.println("RECEIVED " + m);
		}

		exec.shutdown();
		exec.awaitTermination(1, TimeUnit.MINUTES);
	}

	static class Branch implements Runnable {

		UDPNetOverlay ov;

		public Branch(UDPNetOverlay ov) {
			this.ov = ov;
		}

		@Override
		public void run() {
			ov.sendMessage(0, new NetOverlay.Transfer(100));
		}
	}
}
