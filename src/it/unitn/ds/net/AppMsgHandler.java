package it.unitn.ds.net;

import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import it.unitn.ds.net.NetOverlay.Message;

/**
 * Pass incoming app messages to the application
 */
@Sharable
public class AppMsgHandler extends ChannelInboundHandlerAdapter {

	private final UDPNetOverlay no;

	public AppMsgHandler(UDPNetOverlay no) {
		this.no = no;
	}

	@Override
	public void channelRead(ChannelHandlerContext ctx, Object in) throws Exception {
		synchronized (no) {
			no.messageReceived((Message) in);
		}
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		cause.printStackTrace();
	}
}