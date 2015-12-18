import it.unitn.ds.branch.Branch;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

public class Test {

	static int N_BRANCHES = 5;

	static long balance = 1000;

	private static final Map<Integer, InetSocketAddress> branches = new HashMap<Integer, InetSocketAddress>(N_BRANCHES);

	public static void main(String[] args) throws Exception {

		for (int i = 0; i < N_BRANCHES; i++) {
			branches.put(i, new InetSocketAddress(2000 + i));
		}

		Branch[] ov = new Branch[N_BRANCHES];

		for (int i = 0; i < N_BRANCHES; i++) {
			ov[i] = new Branch(i, branches, balance);
			ov[i].start();
		}

		Thread.sleep(500);

		ov[1].startSnapshot(4);

		while (true);
	}
}
