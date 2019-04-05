package com.emc.mongoose.storage.driver.coop.aio;

import com.emc.mongoose.base.item.DataItem;
import com.emc.mongoose.base.item.io.AsyncChannel;
import com.emc.mongoose.base.item.op.data.DataOperation;

import java.nio.ByteBuffer;
import java.nio.channels.CompletionHandler;

public final class CopyCompletionHandler<O extends DataOperation<? extends DataItem>>
implements CompletionHandler<Integer, O> {

	private final AsyncChannel dstChan;
	private final ByteBuffer buff;
	private final long countBytesDone;
	private final O dataOp;
	private final CompletionHandler<Integer, O> opCompletionHandler;

	public CopyCompletionHandler(
		final AsyncChannel dstChan, final ByteBuffer buff, final long countBytesDone, final O dataOp,
		final CompletionHandler<Integer, O> opCompletionHandler
	) {
		this.dstChan = dstChan;
		this.buff = buff;
		this.countBytesDone = countBytesDone;
		this.dataOp = dataOp;
		this.opCompletionHandler = opCompletionHandler;
	}

	@Override
	public void completed(final Integer result, final O attachment) {
		dstChan.write(buff, countBytesDone, dataOp, this);
	}

	@Override
	public void failed(final Throwable exc, final O attachment) {
		opCompletionHandler.failed(exc, attachment);
	}
}
