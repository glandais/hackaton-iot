package com.capgemini.csd.hackaton.server;

import static io.netty.handler.codec.http.HttpHeaders.Names.CONNECTION;
import static io.netty.handler.codec.http.HttpHeaders.Names.CONTENT_LENGTH;

import java.net.InetSocketAddress;

import org.boon.Exceptions;
import org.boon.IO;

import com.capgemini.csd.hackaton.Controler;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponseEncoder;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.util.CharsetUtil;
import io.netty.util.ResourceLeakDetector;
import io.netty.util.ResourceLeakDetector.Level;

public class ServerNetty implements Server {

	static {
		ResourceLeakDetector.setLevel(Level.DISABLED);
	}

	protected class ServerNettyRequestHandler extends SimpleChannelInboundHandler<FullHttpRequest> {

		@Override
		protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest request) throws Exception {
			String uri = request.getUri();
			String message = IO.read(new ByteBufInputStream(request.content()));
			String result = "";
			HttpResponseStatus status = HttpResponseStatus.OK;
			try {
				result = controler.processRequest(uri, message);
			} catch (Exception e) {
				result = Exceptions.asJson(e);
				status = HttpResponseStatus.INTERNAL_SERVER_ERROR;
			}

			FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, status,
					Unpooled.copiedBuffer(result, CharsetUtil.UTF_8));
			response.headers().add(HttpHeaders.Names.CONTENT_TYPE, "application/json");

			if (HttpHeaders.isKeepAlive(request)) {
				response.headers().set(CONTENT_LENGTH, response.content().readableBytes());
				response.headers().set(CONNECTION, HttpHeaders.Values.KEEP_ALIVE);
				//				response.headers().set("Keep-Alive", "timeout=60, max=100000");
				ctx.writeAndFlush(response);
			} else {
				ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
			}
		}

		@Override
		public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
			ctx.close();
		}

		@Override
		public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
			ctx.flush();
		}

	}

	public class ServerNettyChannelInitializer extends ChannelInitializer<SocketChannel> {
		@Override
		public void initChannel(SocketChannel ch) {
			ChannelPipeline p = ch.pipeline();

			p.addLast("encoder", new HttpResponseEncoder());
			p.addLast("decoder", new HttpRequestDecoder(4096, 8192, 8192, false));

			//			p.addLast("inflater", new HttpContentDecompressor());
			//			p.addLast("chunkWriter", new ChunkedWriteHandler());
			//			p.addLast("deflater", new HttpContentCompressor());
			p.addLast("aggregator", new HttpObjectAggregator(1024));

			p.addLast("handler", new ServerNettyRequestHandler());
		}
	}

	private Controler controler;
	private Channel serverChannel;
	private NioEventLoopGroup loupGroup;

	public void start(Controler controler, int port) {
		this.controler = controler;
		// Configure the server.
		//		int threads = 10;// 2 * Runtime.getRuntime().availableProcessors();
		loupGroup = new NioEventLoopGroup();
		try {
			ServerBootstrap b = new ServerBootstrap();
			b.option(ChannelOption.SO_BACKLOG, 1024);
			b.option(ChannelOption.SO_REUSEADDR, true);
			b.group(loupGroup).channel(NioServerSocketChannel.class).childHandler(new ServerNettyChannelInitializer());
			b.option(ChannelOption.MAX_MESSAGES_PER_READ, Integer.MAX_VALUE);
			b.childOption(ChannelOption.ALLOCATOR, new PooledByteBufAllocator(true));
			b.childOption(ChannelOption.SO_REUSEADDR, true);
			b.childOption(ChannelOption.MAX_MESSAGES_PER_READ, Integer.MAX_VALUE);

			//			b.group(bossGroup, workerGroup).channel(NioServerSocketChannel.class)
			//					.handler(new LoggingHandler(LogLevel.INFO))
			//					.childHandler(new ServerNettyChannelInitializer());

			serverChannel = b.bind(new InetSocketAddress(port)).sync().channel();
		} catch (InterruptedException e) {
			throw new IllegalStateException(e);
		}
	}

	public void awaitTermination() {
		try {
			serverChannel.closeFuture().sync();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} finally {
			loupGroup.shutdownGracefully();
		}
	}

	public void close() {
		serverChannel.close();
	}

}
