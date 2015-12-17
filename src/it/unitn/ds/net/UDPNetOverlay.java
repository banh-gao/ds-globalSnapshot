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
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

/**
 * UDP based networking between overlay branches
 * 
 * @author Daniel Zozin
 */
public class UDPNetOverlay implements NetOverlay {

	// Size of thread pool to handle in/out messages
	private static final int POOL_SIZE = 1;

	// Acknowledgement timeout in ms
	private static final int ACK_TIMEOUT = 1000;

	private final EventLoopGroup workersGroup = new NioEventLoopGroup(POOL_SIZE, new ThreadFactory() {

		@Override
		public Thread newThread(Runnable r) {
			Thread t = new Thread(r, "Net Stack Worker");
			t.setDaemon(true);
			return t;
		}
	});

	// Threads used to send messages to remote branches and wait for
	// acknowledgment
	private final ExecutorService senderThreads = Executors.newSingleThreadExecutor(new ThreadFactory() {

		@Override
		public Thread newThread(Runnable r) {
			Thread t = new Thread(r, "Net Sender Worker");
			t.setDaemon(true);
			return t;
		}
	});

	private final Queue<Message> incomingQueue = new ConcurrentLinkedDeque<Message>();

	private final Bootstrap chBoot;

	int localBranch;
	Map<Integer, InetSocketAddress> branches;

	public UDPNetOverlay() {
		chBoot = new Bootstrap();
	}

	@Override
	public void start(int localBranch, Map<Integer, InetSocketAddress> branches) throws IOException, InterruptedException {
		this.localBranch = localBranch;
		this.branches = branches;

		chBoot.group(workersGroup).channel(NioDatagramChannel.class).option(ChannelOption.SO_BROADCAST, true).handler(new StackInitializer());

		InetSocketAddress localAddr = branches.get(localBranch);
		if (localAddr == null)
			throw new IllegalArgumentException("Invalid local branch ID");

		ChannelFuture serverChFut = chBoot.bind(localAddr).sync();

		if (!serverChFut.isSuccess())
			throw new IOException(serverChFut.cause());
	}

	@Override
	public CompletableFuture<Message> sendMessage(int remoteBranch, Message m) {
		m.senderId = localBranch;
		m.destId = remoteBranch;

		CompletableFuture<Message> f = new CompletableFuture<Message>();

		senderThreads.execute(new Runnable() {

			@Override
			public void run() {
				InetSocketAddress remoteAddr = branches.get(remoteBranch);
				if (remoteAddr == null)
					throw new IllegalArgumentException("Invalid branch ID");

				try {
					sendUDPMessage(remoteAddr, m, f);
				} catch (InterruptedException e) {
					// Can never happen
					e.printStackTrace();
				}
			}
		});
		return f;
	}

	private void sendUDPMessage(InetSocketAddress remoteAddr, Message m, CompletableFuture<Message> f) throws InterruptedException {
		Channel ch = chBoot.connect(remoteAddr).sync().channel();
		// Write and wait until message is sent
		ch.writeAndFlush(m).sync();

		LinkHandler linkHandler = ch.pipeline().get(LinkHandler.class);

		// Keeps sending until ack is received
		while (!linkHandler.waitForAck(m.seqn, ACK_TIMEOUT))
			ch.writeAndFlush(m).sync();

		ch.close();
		f.complete(m);
	}

	void messageReceived(Message newMessage) {
		incomingQueue.add(newMessage);
	}

	@Override
	public Message receiveMessage() {
		return incomingQueue.poll();
	}

	@Sharable
	class StackInitializer extends ChannelInitializer<Channel> {

		private final LinkDecoder dec = new LinkDecoder();
		private final LinkDataEncoder enc = new LinkDataEncoder();
		private final LinkHandler lnk = new LinkHandler(localBranch, branches);
		private final AppMsgHandler app = new AppMsgHandler(UDPNetOverlay.this);

		@Override
		protected void initChannel(Channel ch) throws Exception {
			ChannelPipeline pipeline = ch.pipeline();
			// Decoder for incoming messages
			pipeline.addLast(dec);

			// Encoder for outgoing data messages
			pipeline.addLast(enc);

			// Link layer handler to manage link reliability
			pipeline.addLast(lnk);

			// Dispatch incoming messages on the application message bus
			pipeline.addLast(app);
		}
	}
}
