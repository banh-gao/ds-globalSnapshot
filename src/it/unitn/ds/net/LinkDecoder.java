package it.unitn.ds.net;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.socket.DatagramPacket;
import io.netty.handler.codec.MessageToMessageDecoder;
import it.unitn.ds.net.LinkAckEncoder.MessageAck;
import it.unitn.ds.net.NetOverlay.Message;
import it.unitn.ds.net.NetOverlay.Token;
import it.unitn.ds.net.NetOverlay.Transfer;
import java.util.List;

/**
 * Decode all incoming messages
 * 
 * @author Daniel Zozin
 */
@Sharable
public class LinkDecoder extends MessageToMessageDecoder<DatagramPacket> {

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
		byte type = in.readByte();
		if (type == LinkDataEncoder.APP_MONEY_TRANSFER)
			m = new Transfer(in.readLong());
		else if (type == LinkDataEncoder.APP_TOKEN)
			m = new Token(in.readLong());

		if (m == null)
			throw new Exception("Unknown message type");

		m.seqn = seqn;
		m.senderId = senderId;

		return m;
	}
}
