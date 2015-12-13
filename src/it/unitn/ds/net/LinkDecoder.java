package it.unitn.ds.net;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.socket.DatagramPacket;
import io.netty.handler.codec.DecoderException;
import io.netty.handler.codec.MessageToMessageDecoder;
import it.unitn.ds.net.LinkMessage.FramedAck;
import it.unitn.ds.net.LinkMessage.FramedData;
import it.unitn.ds.net.LinkMessage.Type;
import java.util.List;

public class LinkDecoder extends MessageToMessageDecoder<DatagramPacket> {

	@Override
	protected void decode(ChannelHandlerContext ctx, DatagramPacket datagram, List<Object> out) throws Exception {
		ByteBuf in = datagram.content();
		try {
			Type type = Type.valueOf(in.readByte());

			if (type == null)
				throw new DecoderException("Unknown frame type");

			long sequence = in.readUnsignedInt();

			LinkMessage msg = null;

			switch (type) {
				case DATA :
					msg = decodeData(in, sequence);
					break;
				case ACK :
					msg = decodeAck(in, sequence);
					break;
			}
			out.add(msg);
		} finally {
			in.clear();
		}
	}

	public static FramedData decodeData(ByteBuf in, long sequence) {
		return new FramedData(sequence, in.readBytes(AppCodec.MSG_LENGTH));
	}

	public static FramedAck decodeAck(ByteBuf in, long sequence) {
		return new FramedAck(sequence);
	}

}
