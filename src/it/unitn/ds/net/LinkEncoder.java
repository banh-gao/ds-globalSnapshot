package it.unitn.ds.net;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import it.unitn.ds.net.LinkMessage.FramedAck;
import it.unitn.ds.net.LinkMessage.FramedData;
import it.unitn.ds.net.LinkMessage.Type;

/**
 * Codec for RELOAD frame messages exchanged on a link to a neighbor node
 */

public class LinkEncoder extends MessageToByteEncoder<LinkMessage> {

	public LinkEncoder() {
	}

	@Override
	protected void encode(ChannelHandlerContext ctx, LinkMessage msg, ByteBuf out) throws Exception {
		out.writeByte(msg.getType().code);
		out.writeInt((int) msg.getSequence());

		if (msg.getType() == Type.DATA)
			out.writeBytes(((FramedData) msg).getPayload());
	}

	public static void encodeData(FramedData msg, ByteBuf out) {

	}

	public static void encodeAck(FramedAck msg, ByteBuf out) {

	}
}
