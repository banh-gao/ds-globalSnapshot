package it.unitn.ds.net;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Provides the network overlay layer for the distributed banking system
 */
public interface NetOverlay {

	/**
	 * Start the local server by using the given branchId and load all the other
	 * branches addresses. The calling thread will be blocked until the
	 * operation completes.
	 * 
	 * @param localBranch
	 *            the local branchId
	 * @param branches
	 *            A map that associates branches with their network addresses
	 * @throws IOException
	 *             if network initialization failed
	 * @throws InterruptedException
	 */
	void start(int localBranch, Map<Integer, InetSocketAddress> branches) throws IOException, InterruptedException;

	/**
	 * Send the given message to the specified remote branch.
	 * 
	 * @return A completable future that is aynchronously set as completed when
	 *         the message is successfully delivered.
	 * 
	 * @throws InterruptedException
	 */
	<T extends Message> CompletableFuture<T> sendMessage(int remoteBranch, T m);

	/**
	 * @return The next incoming message, if any. Null if there is no message.
	 */
	CompletableFuture<Message> receiveMessage();

	/**
	 * Generic overlay message
	 */
	public abstract class Message {

		int seqn;
		int senderId;
		int destId;

		public int getSenderId() {
			return senderId;
		}

		@Override
		public String toString() {
			return "Message [seqn=" + seqn + ", senderId=" + senderId + "]";
		}
	}

	/**
	 * Money transfer message
	 */
	public class Transfer extends Message {

		private final long amount;

		public Transfer(long amount) {
			this.amount = amount;
		}

		public long getAmount() {
			return amount;
		}

		@Override
		public String toString() {
			return "Transfer [amount=" + amount + ", seqn=" + seqn + ", senderId=" + senderId + "]";
		}
	}

	/**
	 * Snapshot token message
	 */
	public class Token extends Message {

		private final long snapshotId;

		public Token(long snapshotId) {
			this.snapshotId = snapshotId;
		}

		public long getSnapshotId() {
			return snapshotId;
		}

		@Override
		public String toString() {
			return "Token [snapshotId=" + snapshotId + ", seqn=" + seqn + ", senderId=" + senderId + ", destId=" + destId + "]";
		}

	}
}
