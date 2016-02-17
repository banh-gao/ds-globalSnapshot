package it.unitn.ds;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Scanner;
import java.util.function.Function;
import java.util.stream.Collectors;

public class Main {

	static int nextBranchId = 0;
	private static Map<Integer, InetSocketAddress> branches;

	public static void main(String[] args) throws Exception {

		if (args.length < 2) {
			System.out.println("Usage: snapshot.jar <localId> <branchesFile>");
			System.exit(1);
		}

		// Start branch and wait until its started
		int branchId = Integer.parseInt(args[0]);

		parseBranches(args[1]);

		if (branchId >= branches.size()) {
			System.out.println("Invalid branch ID");
			System.exit(1);
		}

		Branch branch = Branch.start(branchId, branches).get();
		System.out.println("Started branch " + branchId + " at " + branches.get(branchId) + " with an initial balance of " + Branch.INITIAL_BALANCE);

		Scanner s = new Scanner(System.in);

		System.out.print("Enter new snapshot ID [Ctrl+D to quit]: ");
		while (s.hasNext()) {
			int snapshotId;
			try {
				snapshotId = s.nextInt();
				// Start snapshot and wait for local completion
				branch.startSnapshot(snapshotId).join();
			} catch (Exception e) {
				System.err.println("Invalid snapshot ID");
				s.nextLine();
			}

			System.out.print("Enter new snapshot ID [Ctrl+D to quit]: ");
		}
		s.close();

		System.out.println("\nShutting down branch activities...");

		branch.stop();

		System.out.println("Program terminated by user");
	}

	private static void parseBranches(String file) throws IOException {
		// Generate branches ID -> internet sockets mapping
		branches = Files.lines(Paths.get(file)).map((l) -> {
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
	}
}
