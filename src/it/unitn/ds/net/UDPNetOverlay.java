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

import java.net.InetSocketAddress;
import java.net.PortUnreachableException;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicReference;

/**
 * UDP based overlay network between branches
 */
public class UDPNetOverlay implements NetOverlay {

	// Size of thread pool to handle in/out messages
	private static final int POOL_SIZE = 1;

	// Acknowledgement timeout in ms
	private static final int ACK_TIMEOUT = 1000;

	private final EventLoopGroup workersGroup = new NioEventLoopGroup(
			POOL_SIZE, new ThreadFactory() {

				@Override
				public Thread newThread(Runnable r) {
					Thread t = new Thread(r, "Net Stack Worker");
					t.setDaemon(true);
					return t;
				}
			});

	private final Queue<Message> incomingQueue = new ConcurrentLinkedDeque<Message>();
	private final Queue<CompletableFuture<Message>> cb = new ConcurrentLinkedDeque<CompletableFuture<Message>>();

	private final Bootstrap chBoot;

	int localBranch;
	Map<Integer, InetSocketAddress> branches;

	private AtomicReference<CompletableFuture<Message>> lastSendFut = new AtomicReference<CompletableFuture<Message>>();

	public UDPNetOverlay() {
		chBoot = new Bootstrap();
	}

	@Override
	public CompletableFuture<Void> start(int localBranch,
			Map<Integer, InetSocketAddress> branches) {
		this.localBranch = localBranch;
		this.branches = branches;

		CompletableFuture<Void> startFut = new CompletableFuture<Void>();

		chBoot.group(workersGroup).channel(NioDatagramChannel.class)
				.option(ChannelOption.SO_BROADCAST, true)
				.handler(new StackInitializer());

		InetSocketAddress localAddr = branches.get(localBranch);
		if (localAddr == null)
			throw new IllegalArgumentException("Invalid local branch ID");

		chBoot.bind(localAddr).addListener((ChannelFuture f) -> {
			if (f.isSuccess())
				startFut.complete(null);
			else
				startFut.completeExceptionally(f.cause());
		});

		return startFut;
	}

	@Override
	public CompletableFuture<Message> sendMessage(
			int remoteBranch, Message msg) {
		
		InetSocketAddress remoteAddr = branches.get(remoteBranch);
		if (remoteAddr == null)
			throw new IllegalArgumentException("Invalid branch ID");
		
		msg.destId = remoteBranch;
		msg.senderId = localBranch;
		msg.deliveryFut = new CompletableFuture<Message>();
		
		// To achieve sequential transmission build a virtual queue by
		// concatenating delivery futures: send the next message only when the
		// previous is delivered
		
		//Atomically change the last delivery future (the tail of the virtual queue)
		CompletableFuture<Message> lastFut = lastSendFut.getAndSet(msg.deliveryFut);
		if (lastFut == null)
			sendUDPMessage(remoteAddr, msg);
		else {
			lastFut.thenRun(() -> sendUDPMessage(remoteAddr, msg));
			//Send current message even if the previous completes exceptionally
			lastFut.exceptionally((ex) -> {
				sendUDPMessage(remoteAddr, msg);
				return null;
			});
		}

		return msg.deliveryFut;
	}

	private void sendUDPMessage(InetSocketAddress remoteAddr, Message m) {
		chBoot.connect(remoteAddr).addListener(new ChannelFutureListener() {

			@Override
			public void operationComplete(ChannelFuture future)
					throws Exception {
				future.channel().writeAndFlush(m);
				// Once the message is delivered close the channel
				m.deliveryFut.thenRun(() -> future.channel().close());
			}
		});
	}

	/**
	 * Notifies waiting dequeuers or enqueue message in incoming queue
	 * 
	 * @param newMessage
	 */
	void messageReceived(Message newMessage) {
		CompletableFuture<Message> f = cb.poll();
		if (f != null)
			f.complete(newMessage);
		else
			incomingQueue.add(newMessage);
	}

	@Override
	public CompletableFuture<Message> receiveMessage() {
		Message m = incomingQueue.poll();
		if (m != null)
			return CompletableFuture.completedFuture(m);
		else {
			CompletableFuture<Message> f = new CompletableFuture<Message>();
			cb.add(f);
			return f;
		}
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
