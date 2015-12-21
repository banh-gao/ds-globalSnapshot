package it.unitn.ds.net;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import it.unitn.ds.net.NetOverlay.Message;
import it.unitn.ds.net.NetOverlay.Token;
import it.unitn.ds.net.NetOverlay.Transfer;

/**
 * Encode outgoing messages
 */
@Sharable
class LinkEncoder extends MessageToByteEncoder<Message> {

	// Link level message types
	public static final byte LNK_DATA = 0x1;

	public static final byte LNK_ACK = 0x2;

	// Application level message types
	public static final byte APP_MONEY_TRANSFER = 0x1;
	public static final byte APP_TOKEN = 0x2;

	@Override
	protected void encode(ChannelHandlerContext ctx, Message msg, ByteBuf out) throws Exception {
		// Link layer header
		if (msg.getClass() == MessageAck.class)
			out.writeByte(LNK_ACK);
		else
			out.writeByte(LNK_DATA);

		out.writeInt(msg.seqn);
		out.writeInt(msg.getSenderId());

		if (msg.getClass() != MessageAck.class)
			encodeDataPayload(msg, out);
	}

	private void encodeDataPayload(Message msg, ByteBuf out) throws Exception {
		if (msg.getClass() == Transfer.class) {
			out.writeByte(APP_MONEY_TRANSFER);
			out.writeLong(((Transfer) msg).getAmount());
		} else if (msg.getClass() == Token.class) {
			out.writeByte(APP_TOKEN);
			out.writeLong(((Token) msg).getSnapshotId());
		} else {
			throw new Exception("Unknown message type");
		}
	}

	static class MessageAck extends Message {

		public MessageAck(int seqn, int senderId) {
			this.seqn = seqn;
			this.senderId = senderId;
		}

		@Override
		public String toString() {
			return "MessageAck [seqn=" + seqn + ", senderId=" + senderId + ", destId=" + destId + "]";
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			MessageAck other = (MessageAck) obj;
			if (senderId != other.senderId)
				return false;
			if (seqn != other.seqn)
				return false;
			return true;
		}
	}
}
