package it.unitn.ds.net;

import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

/**
 * Dispatch incoming messages to a proper handler in a separate thread.
 * The handler is chosen among the registered components ones based on the
 * RELOAD message content type.
 */
@Sharable
public class MessageDispatcher extends ChannelInboundHandlerAdapter {

	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {

	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		System.err.println(cause);
	}

}