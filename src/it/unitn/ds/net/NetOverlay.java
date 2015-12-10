package it.unitn.ds.net;

import java.util.Queue;

public interface NetOverlay {

	/**
	 * Start the local server using the given branchId
	 * 
	 * @param localBranchId
	 */
	void start(int localBranchId);

	/**
	 * Send the given message to a remote branch
	 * 
	 * @return true if the message is successfully acknowledged
	 */
	boolean sendMessage(Message m);

	/**
	 * @return The queue where the incoming messages are enqueued
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

		public Transfer(int senderId) {
			super(senderId);
		}
	}
}
