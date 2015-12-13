package it.unitn.ds.net;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import it.unitn.ds.net.LinkMessage.FramedAck;
import it.unitn.ds.net.LinkMessage.FramedData;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Manage link layer reliability
 * 
 * @author Daniel Zozin
 *
 */
public class LinkHandler extends ChannelDuplexHandler {

	private final int localBranch;

	private ChannelHandlerContext ctx;

	private AtomicInteger nextSeq = new AtomicInteger(0);

	public LinkHandler(int localBranch) {
		this.localBranch = localBranch;
	}

	@Override
	public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
		this.ctx = ctx;
	}

	@Override
	public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
		this.ctx = null;
	}

	@Override
	public void close(ChannelHandlerContext ctx, ChannelPromise future) throws Exception {

	}

	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
		LinkMessage frame = (LinkMessage) msg;
		switch (frame.getType()) {
			case DATA :
				handleData((FramedData) frame);
				ctx.fireChannelRead(((FramedData) frame).getPayload());
				break;
			case ACK :
			default :
				assert false;
				break;
		}
	}

	@Override
	public void write(final ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
		FramedData data = getDataFrame((ByteBuf) msg);

		ctx.write(data, promise);

		if (getLinkTimeout() > 0) {

		}
	}

	private void handleData(FramedData data) {
		// TODO
		ctx.writeAndFlush(null);
	}

	private void handleAck(FramedAck ack) {
		// TODO
	}

	/**
	 * Called when the upper layer want to send a message on the link
	 * 
	 * @param payload
	 * @return
	 */
	protected FramedData getDataFrame(ByteBuf payload) {
		// Prevent overflow
		while (nextSeq.get() == Integer.MAX_VALUE)
			if (nextSeq.compareAndSet(Integer.MAX_VALUE, 0))
				break;

		int seq = nextSeq.getAndIncrement();

		return new FramedData(seq, payload);
	}

	protected long getLinkTimeout() {
		return 0;
	}

	public boolean waitForAck() {
		// TODO Auto-generated method stub
		return false;
	}
}
