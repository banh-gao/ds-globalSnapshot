package it.unitn.ds.net;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.channel.socket.nio.NioDatagramChannel;
import it.unitn.ds.net.LinkAckEncoder.MessageAck;
import it.unitn.ds.net.NetOverlay.Message;
import java.net.InetSocketAddress;
import java.net.PortUnreachableException;
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

	private final Bootstrap ackBoot = new Bootstrap();
	private final int localBranch;
	private final Map<Integer, InetSocketAddress> branches;

	private AtomicInteger nextSeq = new AtomicInteger(1);
	private Map<Integer, Integer> branchesSeqn = new ConcurrentHashMap<Integer, Integer>();

	private MessageAck pendingAck;
	private int lastDelivered;

	public LinkHandler(int localBranch, Map<Integer, InetSocketAddress> branches) {
		this.localBranch = localBranch;
		this.branches = branches;
		ackBoot.channel(NioDatagramChannel.class).handler(new LinkAckEncoder());
	}

	@Override
	public void channelActive(ChannelHandlerContext ctx) throws Exception {
		// Send ACK messages using the channel event loop
		ackBoot.group(ctx.channel().eventLoop());
	}

	@Override
	public void write(final ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
		if (((Message) msg).seqn == 0)
			((Message) msg).seqn = nextSeq();

		pendingAck = new MessageAck(((Message) msg).seqn, ((Message) msg).destId);
		super.write(ctx, msg, promise);
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

		msg.destId = localBranch;

		sendAck(msg);

		// Send to upper layer only if never seen before
		if (branchesSeqn.getOrDefault(msg.senderId, -1) < msg.seqn) {
			branchesSeqn.put(msg.senderId, msg.seqn);
			ctx.fireChannelRead(msg);
		}
	}

	private void sendAck(Message msg) {
		InetSocketAddress branchAddr = branches.get(msg.senderId);
		ackBoot.connect(branchAddr).addListener(new ChannelFutureListener() {

			@Override
			public void operationComplete(ChannelFuture future) throws Exception {
				future.channel().writeAndFlush(new MessageAck(msg.seqn, localBranch)).addListener(new ChannelFutureListener() {

					@Override
					public void operationComplete(ChannelFuture future) throws Exception {
						future.channel().close();
					}
				});
			}
		});
	}

	private void handleAck(MessageAck ack) {
		if (ack.equals(pendingAck)) {
			lastDelivered = pendingAck.seqn;
			synchronized (pendingAck) {
				pendingAck.notifyAll();
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

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		if (cause.getClass() == PortUnreachableException.class) {
			System.err.println("Destination port unreachable");
		}
	}
}
