package it.unitn.ds.net;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioDatagramChannel;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;

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
		// TODO Send udp message to remote branch
		InetSocketAddress remoteAddr = branches.get(remoteBranch);
		if (remoteAddr == null)
			throw new IllegalArgumentException("Invalid branch ID");

		Channel ch = chBoot.connect(remoteAddr).sync().channel();

		ch.writeAndFlush(m);

		LinkHandler linkHandler = ch.pipeline().get(LinkHandler.class);
		return true;
		// return linkHandler.waitForAck();
	}

	@Override
	public Message receiveMessage() throws InterruptedException {
		// TODO Auto-generated method stub
		return null;
	}

	@Sharable
	class StackInitializer extends ChannelInitializer<Channel> {

		@Override
		protected void initChannel(Channel ch) throws Exception {
			ChannelPipeline pipeline = ch.pipeline();
			// Decoder for incoming UDP packets
			pipeline.addLast(new LinkEncoder());

			// Encoder for reliability layer
			pipeline.addLast(new LinkEncoder());

			// Link handler to manage link reliability
			pipeline.addLast(new LinkHandler(localBranch));

			// Codec for applicative message
			pipeline.addLast(new AppCodec());

			// Dispatch incoming messages on the application message bus
			pipeline.addLast(new MessageDispatcher());
		}
	}
}
