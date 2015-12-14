package it.unitn.ds.net;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import it.unitn.ds.net.AckEncoder.MessageAck;
import it.unitn.ds.net.NetOverlay.Message;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Manage link layer reliability
 * 
 * @author Daniel Zozin
 *
 */
public class LinkHandler extends ChannelDuplexHandler {

	private ChannelHandlerContext ctx;

	private AtomicInteger nextSeq = new AtomicInteger(1);
	private Map<Integer, Integer> branchesSeqn = new ConcurrentHashMap<Integer, Integer>();

	MessageAck pendingAck;
	private int lastDelivered;

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
	public void write(final ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
		// Prevent overflow
		while (nextSeq.get() == Integer.MAX_VALUE)
			if (nextSeq.compareAndSet(Integer.MAX_VALUE, 0))
				break;

		((Message) msg).seqn = nextSeq.getAndIncrement();
		pendingAck = new MessageAck(((Message) msg).seqn, ((Message) msg).senderId);

		ctx.write(msg, promise);
	}

	@Override
	public void channelRead(ChannelHandlerContext ctx, Object in) throws Exception {
		if (in.getClass() == MessageAck.class) {
			handleAck((MessageAck) in);
			return;
		}

		Message msg = (Message) in;

		// Send to upper layer only if never seen before
		if (branchesSeqn.getOrDefault(msg.senderId, -1) < msg.seqn) {
			branchesSeqn.put(msg.senderId, msg.seqn);
			ctx.fireChannelRead(msg);
		}
	}

	private void handleAck(MessageAck ack) {
		System.out.println(ack);
		if (ack.equals(pendingAck))
			synchronized (pendingAck) {
				pendingAck.notifyAll();
			}
	}

	public boolean waitForAck(int pendingSeqn, int timeout) throws InterruptedException {
		if (pendingAck == null)
			return false;

		if (pendingAck.seqn > pendingSeqn)
			return true;

		synchronized (pendingAck) {
			pendingAck.wait(timeout);
		}

		return (lastDelivered == pendingSeqn);
	}
}
