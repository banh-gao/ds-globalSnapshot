package it.unitn.ds.net;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioDatagramChannel;
import it.unitn.ds.net.AckEncoder.MessageAck;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * UDP based networking between overlay branches
 * 
 * @author Daniel Zozin
 */
public class UDPNetOverlay implements NetOverlay {

	private static final int POOL_SIZE = 1;

	private final EventLoopGroup workersGroup = new NioEventLoopGroup(POOL_SIZE);

	private final Bootstrap chBoot;

	private int localBranch;

	private Map<Integer, InetSocketAddress> branches;

	private final BlockingQueue<Message> incomingQueue = new LinkedBlockingQueue<NetOverlay.Message>();

	public UDPNetOverlay() {
		chBoot = new Bootstrap();
		chBoot.group(workersGroup).channel(NioDatagramChannel.class).option(ChannelOption.SO_BROADCAST, true).handler(new StackInitializer());
	}

	@Override
	public void start(int localBranch, Map<Integer, InetSocketAddress> branches) throws IOException, InterruptedException {
		this.localBranch = localBranch;
		this.branches = branches;

		InetSocketAddress localAddr = branches.get(localBranch);
		if (localAddr == null)
			throw new IllegalArgumentException("Invalid local branch ID");

		ChannelFuture serverChFut = chBoot.bind(localAddr).sync();

		if (!serverChFut.isSuccess())
			throw new IOException(serverChFut.cause());
	}

	@Override
	public boolean sendMessage(int remoteBranch, Message m) throws InterruptedException {
		m.senderId = localBranch;

		// TODO Send udp message to remote branch
		InetSocketAddress remoteAddr = branches.get(remoteBranch);
		if (remoteAddr == null)
			throw new IllegalArgumentException("Invalid branch ID");

		Channel ch = chBoot.connect(remoteAddr).sync().channel();
		ch.writeAndFlush(m);

		LinkHandler linkHandler = ch.pipeline().get(LinkHandler.class);
		return true;
		// TODO: wait for ack return linkHandler.waitForAck();
	}

	void messageReceived(Message newMessage) {
		boolean accepted = false;
		synchronized (incomingQueue) {
			accepted = incomingQueue.offer(newMessage);
		}
		if (accepted)
			sendAck(newMessage);
	}

	private void sendAck(Message msg) {
		Bootstrap chBoot = new Bootstrap();
		chBoot.group(workersGroup).channel(NioDatagramChannel.class).handler(new AckEncoder());

		InetSocketAddress branchAddr = branches.get(msg.senderId);
		chBoot.connect(branchAddr).addListener(new ChannelFutureListener() {

			@Override
			public void operationComplete(ChannelFuture future) throws Exception {
				future.channel().writeAndFlush(new MessageAck(msg.seqn, localBranch));
			}
		});
	}

	@Override
	public Message receiveMessage() throws InterruptedException {
		return incomingQueue.take();
	}

	@Sharable
	class StackInitializer extends ChannelInitializer<Channel> {

		@Override
		protected void initChannel(Channel ch) throws Exception {
			ChannelPipeline pipeline = ch.pipeline();
			// Decoder for incoming messages
			pipeline.addLast(new LinkDecoder());

			// Encoder for outgoing data messages
			pipeline.addLast(new DataEncoder());

			// Link layer handler to manage link reliability
			pipeline.addLast(new LinkHandler());

			// Dispatch incoming messages on the application message bus
			pipeline.addLast(new AppMsgHandler(UDPNetOverlay.this));
		}
	}
}
