package it.unitn.ds.net;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import it.unitn.ds.net.AckEncoder.MessageAck;

public class AckEncoder extends MessageToByteEncoder<MessageAck> {

	public static final byte LNK_ACK = 0x2;

	@Override
	protected void encode(ChannelHandlerContext ctx, MessageAck msg, ByteBuf out) throws Exception {
		// Link layer header
		out.writeByte(LNK_ACK);
		out.writeInt(msg.seqn);
		out.writeInt(msg.senderId);
	}

	static class MessageAck {

		int seqn;
		int senderId;

		public MessageAck(int seqn, int senderId) {
			this.seqn = seqn;
			this.senderId = senderId;
		}

		@Override
		public String toString() {
			return "MessageAck [seqn=" + seqn + ", senderId=" + senderId + "]";
		}
	}
}
