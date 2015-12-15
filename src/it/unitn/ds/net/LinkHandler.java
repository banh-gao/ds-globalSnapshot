package it.unitn.ds.net;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.channel.socket.nio.NioDatagramChannel;
import it.unitn.ds.net.AckEncoder.MessageAck;
import it.unitn.ds.net.NetOverlay.Message;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Manage link layer reliability
 * 
 * @author Daniel Zozin
 *
 */
@Sharable
public class LinkHandler extends ChannelDuplexHandler {

	private final AckEncoder ackEnc = new AckEncoder();
	private final int localBranch;
	private final Map<Integer, InetSocketAddress> branches;

	private ChannelHandlerContext ctx;

	private AtomicInteger nextSeq = new AtomicInteger(1);
	private Map<Integer, Integer> branchesSeqn = new ConcurrentHashMap<Integer, Integer>();

	private MessageAck pendingAck;
	private int lastDelivered;

	public LinkHandler(int localBranch, Map<Integer, InetSocketAddress> branches) {
		this.localBranch = localBranch;
		this.branches = branches;
	}

	@Override
	public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
		this.ctx = ctx;
	}

	@Override
	public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
	}

	@Override
	public void close(ChannelHandlerContext ctx, ChannelPromise future) throws Exception {
	}

	@Override
	public void write(final ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
		if (((Message) msg).seqn == 0)
			((Message) msg).seqn = nextSeq();

		pendingAck = new MessageAck(((Message) msg).seqn, ((Message) msg).destId);
		ctx.write(msg, promise);
	}

	private int nextSeq() {
		// Prevent overflow
		while (nextSeq.get() == Integer.MAX_VALUE)
			if (nextSeq.compareAndSet(Integer.MAX_VALUE, 0))
				break;

		return nextSeq.getAndIncrement();
	}

	@Override
	public void channelRead(ChannelHandlerContext ctx, Object in) throws Exception {
		if (in.getClass() == MessageAck.class) {
			handleAck((MessageAck) in);
			return;
		}

		Message msg = (Message) in;

		sendAck(msg);

		// Send to upper layer only if never seen before
		if (branchesSeqn.getOrDefault(msg.senderId, -1) < msg.seqn) {
			branchesSeqn.put(msg.senderId, msg.seqn);
			ctx.fireChannelRead(msg);
		}
	}

	private void sendAck(Message msg) {
		Bootstrap chBoot = new Bootstrap();
		chBoot.group(ctx.channel().eventLoop()).channel(NioDatagramChannel.class).handler(ackEnc);
		InetSocketAddress branchAddr = branches.get(msg.senderId);
		chBoot.connect(branchAddr).addListener(new ChannelFutureListener() {

			@Override
			public void operationComplete(ChannelFuture future) throws Exception {
				future.channel().writeAndFlush(new MessageAck(msg.seqn, localBranch));
			}
		});
	}

	private void handleAck(MessageAck ack) {
		if (ack.equals(pendingAck)) {
			lastDelivered = pendingAck.seqn;
			synchronized (pendingAck) {
				pendingAck.notifyAll();
				pendingAck = null;
			}
		}
	}

	public boolean waitForAck(int pendingSeqn, int timeout) throws InterruptedException {
		if (pendingAck == null)
			return false;

		synchronized (pendingAck) {
			pendingAck.wait(timeout);
		}

		return (lastDelivered == pendingSeqn);
	}
}
