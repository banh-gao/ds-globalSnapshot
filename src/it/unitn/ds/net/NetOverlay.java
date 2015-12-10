package it.unitn.ds.net;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Queue;

public interface NetOverlay {

	/**
	 * Start the local server by using the given branchId and load all the other
	 * branches addresses
	 * 
	 * @param localBranch
	 *            the local branchId
	 * @param branches
	 *            A map that associates branches with their network addresses
	 * @return true if the server has started, false otherwise
	 */
	boolean start(int localBranch, Map<Integer, InetSocketAddress> branches);

	/**
	 * Send the given message to the specified remote branch
	 * 
	 * @return true if the message was successfully delivered, false otherwise
	 */
	boolean sendMessage(int remoteBranch, Message m);

	/**
	 * @return The queue where the incoming messages are enqueued, ordered by
	 *         reception time
	 */
	Queue<? extends Message> getIncomingQueue();

	/**
	 * Generic overlay message
	 */
	abstract class Message {

		private final int senderId;

		public Message(int senderId) {
			this.senderId = senderId;
		}

		int getSenderId() {
			return senderId;
		}
	}

	/**
	 * Money transfer message
	 */
	class Transfer extends Message {

		private final long amount;

		public Transfer(int senderId, long amount) {
			super(senderId);
			this.amount = amount;
		}

		public long getAmount() {
			return amount;
		}
	}
}
