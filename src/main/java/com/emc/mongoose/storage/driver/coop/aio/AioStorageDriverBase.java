package com.emc.mongoose.storage.driver.coop.aio;

import com.emc.mongoose.base.config.IllegalConfigurationException;
import com.emc.mongoose.base.data.DataInput;
import com.emc.mongoose.base.item.DataItem;
import com.emc.mongoose.base.item.Item;
import com.emc.mongoose.base.item.io.AsyncChannel;
import com.emc.mongoose.base.item.op.Operation;
import com.emc.mongoose.base.item.op.data.DataOperation;
import com.emc.mongoose.base.item.op.path.PathOperation;
import com.emc.mongoose.base.logging.LogUtil;
import com.emc.mongoose.base.logging.Loggers;
import com.emc.mongoose.storage.driver.coop.CoopStorageDriverBase;

import static com.emc.mongoose.base.item.op.Operation.Status.ACTIVE;
import static com.emc.mongoose.base.item.op.Operation.Status.FAIL_IO;
import static com.emc.mongoose.base.item.op.Operation.Status.PENDING;

import com.github.akurilov.commons.system.DirectMemUtil;
import com.github.akurilov.confuse.Config;

import org.apache.logging.log4j.Level;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.CompletionHandler;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public abstract class AioStorageDriverBase<I extends Item, O extends Operation<I>>
extends CoopStorageDriverBase<I, O>
implements CompletionHandler<Integer, DataOperation<? extends DataItem>> {

	private final Map<DataOperation, AsyncChannel> srcOpenChannels = new ConcurrentHashMap<>();
	private final Map<DataOperation, AsyncChannel> dstOpenChannels = new ConcurrentHashMap<>();

	protected AioStorageDriverBase(
		final String testStepId, final DataInput dataInput, final Config storageConfig, final boolean verifyFlag,
		final int batchSize
	) throws IllegalConfigurationException {
		super(testStepId, dataInput, storageConfig, verifyFlag, batchSize);
	}

	@Override
	protected boolean submit(final O op)
	throws IllegalStateException {
		final var status = op.status();
		if(PENDING.equals(status)) {
			if(concurrencyThrottle.tryAcquire()) {
				Loggers.MSG.trace("{}: start operation \"{}\"", stepId, op);
				op.startRequest();
			} else {
				return false;
			}
		} else if(ACTIVE.equals(status)){
			Loggers.MSG.trace("{}: continue the operation \"{}\"", stepId, op);
		} else {
			return true; // dirty fix
		}

		if (op instanceof DataOperation) {
			submitDataOperation((DataOperation<? extends DataItem>) op);
		} else if (op instanceof PathOperation) {
			throw new AssertionError("Not implemented");
		} else {
			throw new AssertionError("Not implemented");
		}
		op.finishRequest();
		return true;
	}

	@SuppressWarnings("unchecked")
	final void submitDataOperation(final DataOperation<? extends DataItem> op) {

		var srcChannel = (AsyncChannel) null;
		var dstChannel = (AsyncChannel) null;

		final var opType = op.type();
		final var item = op.item();

		try {
			switch (opType) {
				case NOOP:
					completeOperation((O) op, null, null);
					break;
				case CREATE:
					dstChannel = destinationChannel(op);
					if (dstChannel == null) {
						break;
					}
					srcChannel = sourceChannel(op);
					if (srcChannel == null) {
						if (op.status().equals(Operation.Status.FAIL_IO)) {
							break;
						} else {
							invokeCreateData(op, dstChannel);
						}
					} else {
						invokeCopyData(op, srcChannel, dstChannel);
					}
					break;
				case READ:
					throw new AssertionError("Not implemented yet");
				case UPDATE:
					throw new AssertionError("Not implemented yet");
				case DELETE:
					throw new AssertionError("Not implemented yet");
				case LIST:
					throw new AssertionError("Not implemented yet");
			}
		} catch(final IOException e) {
			LogUtil.exception(Level.WARN, e, "{}: failed to invoke the load operation {}", stepId, op);
		}
	}

	@Override
	protected final int submit(final List<O> ops, final int from, final int to)
	throws IllegalStateException {
		for(var i = from; i < to; i ++) {
			if(!submit(ops.get(i))) {
				return i - from;
			}
		}
		return to - from;
	}

	@Override
	protected final int submit(final List<O> ops)
	throws IllegalStateException {
		return submit(ops, 0, ops.size());
	}

	protected abstract AsyncChannel openSourceChannel(final DataOperation<? extends DataItem> dataOp);

	protected final AsyncChannel sourceChannel(final DataOperation<? extends DataItem> dataOp) {
		return srcOpenChannels.computeIfAbsent(dataOp, this::openSourceChannel);
	}

	protected abstract AsyncChannel openDestinationChannel(final DataOperation<? extends DataItem> dataOp);

	protected final AsyncChannel destinationChannel(final DataOperation<? extends DataItem> dataOp) {
		return dstOpenChannels.computeIfAbsent(dataOp, this::openDestinationChannel);
	}

	@SuppressWarnings("unchecked")
	protected final void invokeCreateData(final DataOperation dataOp, final AsyncChannel dstChan)
	throws IOException {
		Loggers.MSG.trace("{}: invoke create data \"{}\"", stepId, dataOp);
		final var dataItem = dataOp.item();
		final var countBytesDone = dataOp.countBytesDone();
		final var contentSize = dataItem.size();
		if (countBytesDone < contentSize) {
			dataItem.writeToAsyncChannel(
				dstChan, countBytesDone, contentSize - countBytesDone, dataOp,
				((CompletionHandler<Integer, ? super DataOperation>) ((Object) this))
			);
		} else {
			completeOperation((O) dataOp, null, dstChan);
		}
	}

	@SuppressWarnings("unchecked")
	protected final void invokeCopyData(
		final DataOperation dataOp, final AsyncChannel srcChan, final AsyncChannel dstChan
	) throws IOException {
		final var dataItem = dataOp.item();
		final var countBytesDone = dataOp.countBytesDone();
		final var contentSize = dataItem.size();
		final var countBytesRemaining = contentSize - countBytesDone;
		if (countBytesRemaining > 0) {
			final var buff = DirectMemUtil.getThreadLocalReusableBuff(countBytesRemaining);
			final var copyCompletionHandler = copyCompletionHandler(dstChan, buff, countBytesDone, dataOp);
			srcChan.read(buff, countBytesDone, dataOp, copyCompletionHandler);
		} else {
			completeOperation((O) dataOp, srcChan, dstChan);
		}
	}

	@SuppressWarnings("unchecked")
	<F extends DataItem, D extends DataOperation<F>> CompletionHandler<Integer, D> copyCompletionHandler(
		final AsyncChannel dstChan, final ByteBuffer buff, final long countBytesDone, final D dataOp
	) {
		return new CopyCompletionHandler<>(dstChan, buff, countBytesDone, dataOp, (CompletionHandler<Integer, D>) this);
	}

	/**
	 Invoked on the operation invocation completion. An operation may require many such invocations before its
	 completion.
	 @param n
	 @param dataOp
	 */
	@Override @SuppressWarnings("unchecked")
	public void completed(final Integer n, final DataOperation<? extends DataItem> dataOp) {
		Loggers.MSG.trace("{}: operation \"{}\" invocation completed with the result: {}", stepId, dataOp, n);
		final var newCountBytesDone = dataOp.countBytesDone() + n;
		dataOp.countBytesDone(newCountBytesDone);
		dataOp.item().position(newCountBytesDone); // TODO correct only for full create/full read
		if(!childOpsQueue.offer((O) dataOp)) {
			Loggers.ERR.error("{}: failed to resubmit the load operation, the result will be lost", toString());
		}
	}

	@Override @SuppressWarnings("unchecked")
	public void failed(final Throwable thrown, final DataOperation<? extends DataItem> dataOp) {
		try {
			LogUtil.exception(Level.ERROR, thrown, "{}: data load operation \"{}\" failure", stepId, dataOp);
			dataOp.status(FAIL_IO);
			handleCompleted((O) dataOp);
		} finally {
			concurrencyThrottle.release();
		}
	}

	protected void completeOperation(final O op, final AsyncChannel srcChannel, final AsyncChannel dstChannel) {
		Loggers.MSG.trace("{}: operation \"{}\" completed", stepId, op);
		try {
			op.startResponse();
			op.finishResponse();
			op.status(Operation.Status.SUCC);
		} catch (final IllegalStateException e) {
			LogUtil.exception(
				Level.WARN, e, "{}: finishing the load operation which is in an invalid state", op.toString());
			op.status(Operation.Status.FAIL_UNKNOWN);
		} finally {
			try {
				handleCompleted(op);
			} finally {
				concurrencyThrottle.release();
				Loggers.MSG.trace("{}: operation \"{}\" released the concurrency throttle", stepId, op);
				if (srcChannel != null) {
					srcOpenChannels.remove(op);
					if (srcChannel.isOpen()) {
						try {
							srcChannel.close();
						} catch (final IOException e) {
							Loggers.ERR.warn("Failed to close the source file channel");
						}
					}
				}
				if (dstChannel != null) {
					dstOpenChannels.remove(op);
					if (dstChannel.isOpen()) {
						try {
							dstChannel.close();
						} catch (final IOException e) {
							Loggers.ERR.warn("Failed to close the destination file channel");
						}
					}
				}
			}
		}
	}

	@Override
	protected void doClose()
	throws IOException {

		srcOpenChannels
			.values()
			.stream()
			.filter(AsyncChannel::isOpen)
			.forEach(
				channel -> {
					try {
						channel.close();
					} catch (final IOException e) {
						LogUtil.exception(Level.WARN, e, "Failed to close the source file channel {}", channel);
					}
				});
		srcOpenChannels.clear();

		dstOpenChannels
			.values()
			.stream()
			.filter(AsyncChannel::isOpen)
			.forEach(
				channel -> {
					try {
						channel.close();
					} catch (final IOException e) {
						LogUtil.exception(Level.WARN, e, "Failed to close the source file channel {}", channel);
					}
				});
		dstOpenChannels.clear();

		super.doClose();
	}
}
