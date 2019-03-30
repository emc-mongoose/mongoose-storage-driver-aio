package com.emc.mongoose.storage.driver.coop.aio;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousByteChannel;
import java.nio.channels.AsynchronousChannel;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;

public final class AsyncChannelWrapper
implements AsyncChannel {

	private final AsynchronousChannel channel;

	public AsyncChannelWrapper(final AsynchronousChannel channel) {
		this.channel = channel;
	}

	@Override
	public final <A> void read(
		final ByteBuffer dst, final A attach, final CompletionHandler<Integer,? super A> handler
	) {
		((AsynchronousByteChannel) channel).read(dst, attach, handler);
	}

	@Override
	public final <A> void read(
		final ByteBuffer dst, final long position, final A attach,
		final CompletionHandler<Integer,? super A> handler
	) {
		((AsynchronousFileChannel) channel).read(dst, position, attach, handler);
	}

	@Override
	public final <A> void write(
		final ByteBuffer src, final A attach, final CompletionHandler<Integer,? super A> handler
	) {
		((AsynchronousByteChannel) channel).write(src, attach, handler);
	}

	@Override
	public final <A> void write(
		final ByteBuffer src, final long position, final A attach, final CompletionHandler<Integer,? super A> handler
	) {
		((AsynchronousFileChannel) channel).write(src, position, attach, handler);
	}

	@Override
	public final boolean isOpen() {
		return channel.isOpen();
	}

	@Override
	public final void close()
	throws IOException {
		channel.close();
	}
}
