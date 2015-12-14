package it.unitn.ds.net;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import it.unitn.ds.net.NetOverlay.Message;
import it.unitn.ds.net.NetOverlay.Transfer;

/**
 * Codec for RELOAD frame messages exchanged on a link to a neighbor node
 */
@Sharable
class DataEncoder extends MessageToByteEncoder<Message> {

	// Link level message types
	public static final byte LNK_DATA = 0x1;

	// Application level message types
	public static final byte APP_MONEY_TRANSFER = 0x1;

	@Override
	protected void encode(ChannelHandlerContext ctx, Message msg, ByteBuf out) throws Exception {
		// Link layer header
		out.writeByte(LNK_DATA);
		out.writeInt(msg.seqn);
		out.writeInt(msg.getSenderId());

		encodeDataPayload(msg, out);
	}

	private void encodeDataPayload(Message msg, ByteBuf out) throws Exception {
		if (msg.getClass() == Transfer.class) {
			out.writeByte(APP_MONEY_TRANSFER);
			out.writeLong(((Transfer) msg).getAmount());
		} else {
			throw new Exception("Unknown message type");
		}
	}
}
