package it.unitn.ds.net;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * Provides the network overlay layer for the distributed banking system
 * 
 * @author Daniel Zozin
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
	 * @param exec
	 *            The executor to be used to process incoming messages and
	 *            completion of message sending operations
	 * @throws IOException
	 *             if network initialization failed
	 * @throws InterruptedException
	 */
	void start(int localBranch, Map<Integer, InetSocketAddress> branches, Executor exec) throws IOException, InterruptedException;

	/**
	 * Send the given message to the specified remote branch. The message will
	 * be queued and sent the returned future is completed once the message is
	 * successfully delivered
	 * 
	 * @return The future used to indicate the completion of the delivery
	 */
	CompletableFuture<Message> sendMessage(int remoteBranch, Message m);

	/**
	 * @return A future that supplies the next incoming message, or null if
	 *         there is no message
	 */
	CompletableFuture<Message> receiveMessage();

	/**
	 * Generic overlay message
	 */
	abstract class Message {

		int seqn;
		int senderId;
		int destId;

		int getSenderId() {
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
	class Transfer extends Message {

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
}
