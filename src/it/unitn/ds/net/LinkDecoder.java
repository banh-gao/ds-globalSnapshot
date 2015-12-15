package it.unitn.ds.net;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.socket.DatagramPacket;
import io.netty.handler.codec.MessageToMessageDecoder;
import it.unitn.ds.net.LinkAckEncoder.MessageAck;
import it.unitn.ds.net.NetOverlay.Message;
import it.unitn.ds.net.NetOverlay.Transfer;
import java.util.List;

/**
 * Decode all incoming messages
 * 
 * @author Daniel Zozin
 */
@Sharable
public class LinkDecoder extends MessageToMessageDecoder<DatagramPacket> {

	private static final byte MSG_TRANFER = 0x1;

	@Override
	protected void decode(ChannelHandlerContext ctx, DatagramPacket datagram, List<Object> out) throws Exception {
		ByteBuf in = datagram.content();
		try {
			// Decode link layer header
			int msgType = in.readByte();
			int seqn = in.readInt();
			int senderId = in.readInt();

			switch (msgType) {
				case LinkDataEncoder.LNK_DATA :
					out.add(decodeData(in, seqn, senderId));
					break;
				case LinkAckEncoder.LNK_ACK :
					out.add(new MessageAck(seqn, senderId));
					break;
			}
		} finally {
			in.clear();
		}
	}

	public static Message decodeData(ByteBuf in, int seqn, int senderId) throws Exception {
		Message m = null;
		if (in.readByte() == MSG_TRANFER)
			m = new Transfer(in.readLong());

		if (m == null)
			throw new Exception("Unknown message type");

		m.seqn = seqn;
		m.senderId = senderId;

		return m;
	}
}
