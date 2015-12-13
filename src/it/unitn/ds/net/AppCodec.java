package it.unitn.ds.net;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageCodec;
import it.unitn.ds.net.NetOverlay.Message;
import it.unitn.ds.net.NetOverlay.Transfer;
import java.util.List;

/**
 * 
 * @author Daniel Zozin
 *
 */
public class AppCodec extends ByteToMessageCodec<Message> {

	// Sender id (4 bytes) + msg type (1 byte) + app data (8 bytes)
	public static final int MSG_LENGTH = 13;

	private static final byte MSG_TRANFER = 0x1;

	@Override
	protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
		try {
			int senderId = in.readInt();

			switch (in.readByte()) {
				case MSG_TRANFER :

					out.add(new Transfer(senderId, in.readLong()));
				default :
					throw new Exception("Unknown message type");
			}
		} finally {
			in.clear();
		}
	}

	@Override
	protected void encode(ChannelHandlerContext ctx, Message msg, ByteBuf out) throws Exception {
		// 4 bytes
		out.writeInt(msg.getSenderId());

		if (msg.getClass() == Transfer.class) {
			// 1 bytes
			out.writeByte(MSG_TRANFER);

			// 8 bytes
			out.writeLong(((Transfer) msg).getAmount());
		} else {
			throw new Exception("Unknown message type");
		}
	}
}
