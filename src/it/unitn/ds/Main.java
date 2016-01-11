package it.unitn.ds;

import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;

public class Main {

	static int nextBranchId = 0;
	private static Map<Integer, InetSocketAddress> branches;

	public static void main(String[] args) throws Exception {

		if (args.length < 1) {
			System.out.println("Usage: snapshot.jar <branchesFile>");
			System.exit(1);
		}

		// Generate branches ID -> internet sockets mapping
		branches = Files.lines(Paths.get(args[0])).map((l) -> {
			String[] v = l.split(" ");
			if (v.length < 2) {
				System.err.println("Invalid branch format " + l);
				return null;
			}

			try {
				return new InetSocketAddress(v[0], Integer.parseInt(v[1]));
			} catch (Exception e) {
				System.err.println("Invalid branch format " + l);
				return null;
			}
		}).filter((a) -> a != null).collect(Collectors.toMap((a) -> nextBranchId++, Function.identity()));

		System.out.println("Loaded " + branches.size() + " branches");

		@SuppressWarnings("unchecked")
		CompletableFuture<Branch>[] b = new CompletableFuture[branches.size()];

		// Start all branches in parallel
		branches.keySet().forEach(branchId -> {
			b[branchId] = Branch.start(branchId, branches);
			b[branchId].thenRun(() -> {
				System.out.println("Started branch " + branchId + " at " + branches.get(branchId));
			});
		});

		// Wait until all branches are started
		CompletableFuture.allOf(b).join();

		System.out.println("Started " + b.length + " branches with a global initial balance of " + branches.size() * Branch.INITIAL_BALANCE);

		Scanner s = new Scanner(System.in);

		System.out.print("Enter new snapshot ID [Ctrl+D to quit]: ");
		while (s.hasNext()) {
			int snapshotId;
			try {
				snapshotId = s.nextInt();

				GlobalSnapshotCollector.initSnapshot(branches.size());

				// Always start snapshot from branch 0 (It can start everywhere
				// but
				// concurrent snapshots are not supported by branches)
				b[0].get().startSnapshot(snapshotId);

				// Wait until the snapshot terminates globally
				long globalBalance = GlobalSnapshotCollector.getGlobalBalance().get();

				System.out.println("Snapshot " + snapshotId + " reports a global balance of " + globalBalance);

			} catch (Exception e) {
				System.err.println("Invalid snapshot ID");
				s.nextLine();
			}

			System.out.print("Enter new snapshot ID [Ctrl+D to quit]: ");
		}
		s.close();

		System.out.println("\nProgram terminated by user");
	}
}
