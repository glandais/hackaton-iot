package com.capgemini.csd.hackaton.client;

import java.util.Date;
import java.util.concurrent.SynchronousQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.util.CharsetUtil;
import io.netty.util.concurrent.DefaultPromise;
import io.netty.util.concurrent.Promise;

// ne fonctionne pas (on renvoie une requete avant d'avoir recu une reponse...)
public class ClientNetty extends AbstractClient {

	public final static Logger LOGGER = LoggerFactory.getLogger(ClientNetty.class);

	protected SynchronousQueue<Promise<Object>> queue = new SynchronousQueue<>(true);

	public class ClientNettyResponseHandler extends ChannelDuplexHandler {

		@Override
		public void channelRead(ChannelHandlerContext ctx, Object msg) {
			if (msg instanceof HttpResponse) {
				LOGGER.info(msg.toString());
				queue.remove().setSuccess(msg);
			}
		}

		@Override
		public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
			queue.remove().setFailure(cause);
			ctx.fireExceptionCaught(cause);
		}
	}

	public class ClientNettyInitializer extends ChannelInitializer<SocketChannel> {

		@Override
		public void initChannel(SocketChannel ch) {
			ChannelPipeline p = ch.pipeline();

			p.addLast(new HttpClientCodec());

			//			p.addLast("aggregator", new HttpObjectAggregator(20480));

			// Remove the following line if you don't want automatic content decompression.
			//			p.addLast(new HttpContentDecompressor());
			// Uncomment the following line if you don't want to handle HttpContents.
			//p.addLast(new HttpObjectAggregator(1048576));
			p.addLast(new ClientNettyResponseHandler());
		}
	}

	private Bootstrap b;
	private NioEventLoopGroup group;

	private ThreadLocal<Channel> channel = ThreadLocal.withInitial(() -> {
		try {
			Channel ch = b.connect(host, port).sync().channel();
			ch.closeFuture().addListener(f -> removeChannel());
			return ch;
		} catch (InterruptedException e) {
			throw new IllegalStateException(e);
		}
	});

	public ClientNetty() {
		group = new NioEventLoopGroup();
		b = new Bootstrap();
		b.group(group).channel(NioSocketChannel.class).handler(new ClientNettyInitializer());
	}

	public void removeChannel() {
		channel.remove();
	}

	@Override
	public void sendMessage(String message) {
		try {
			Channel ch = channel.get();
			HttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/messages",
					Unpooled.copiedBuffer(message, CharsetUtil.UTF_8));
			HttpHeaders.setKeepAlive(request, true);
			Promise<Object> promise = new DefaultPromise<>(ch.eventLoop());
			promise.addListener(f -> {
				System.out.println("Finished");
			});
			ch.writeAndFlush(request).sync();
			queue.offer(promise);
			Object response = promise.get();
			LOGGER.info("Réponse : " + response);
		} catch (Exception e) {
			LOGGER.error("Echec à l'envoi du message", e);
			channel.remove();
		}
	}

	@Override
	public String getSynthese(Date start, int duration) {
		return "?";
	}

	@Override
	public void shutdown() {
		group.shutdownGracefully();
	}

}
