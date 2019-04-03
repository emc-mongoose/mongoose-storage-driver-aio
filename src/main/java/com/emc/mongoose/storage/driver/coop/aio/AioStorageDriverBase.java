package com.emc.mongoose.storage.driver.coop.aio;

import com.emc.mongoose.base.config.IllegalConfigurationException;
import com.emc.mongoose.base.data.DataInput;
import com.emc.mongoose.base.item.DataItem;
import com.emc.mongoose.base.item.Item;
import com.emc.mongoose.base.item.io.AsyncChannel;
import com.emc.mongoose.base.item.op.Operation;
import com.emc.mongoose.base.item.op.data.DataOperation;
import com.emc.mongoose.base.logging.LogUtil;
import com.emc.mongoose.storage.driver.coop.CoopStorageDriverBase;
import com.github.akurilov.confuse.Config;
import org.apache.logging.log4j.Level;

import java.io.IOException;
import java.nio.channels.CompletionHandler;

import static com.emc.mongoose.base.item.op.Operation.Status.ACTIVE;
import static com.emc.mongoose.base.item.op.Operation.Status.FAIL_IO;
import static com.emc.mongoose.base.item.op.Operation.Status.SUCC;

public abstract class AioStorageDriverBase<I extends Item, O extends Operation<I>>
extends CoopStorageDriverBase<I, O>
implements CompletionHandler<Integer, DataOperation<? extends DataItem>> {

	protected AioStorageDriverBase(
		final String testStepId, final DataInput dataInput, final Config storageConfig, final boolean verifyFlag,
		final int batchSize
	) throws IllegalConfigurationException {
		super(testStepId, dataInput, storageConfig, verifyFlag, batchSize);
	}

	protected abstract AsyncChannel openDstChan(final DataOperation<? extends DataItem> dataOp);

	@SuppressWarnings("unchecked")
	protected final void invokeCreate(
		final DataItem fileItem, final DataOperation dataOp, final AsyncChannel dstChannel
	) throws IOException {
		final var countBytesDone = dataOp.countBytesDone();
		final var contentSize = fileItem.size();
		if(ACTIVE.equals(dataOp.status())) {
			if (countBytesDone < contentSize) {
				fileItem.writeToAsyncChannel(dstChannel, countBytesDone, contentSize - countBytesDone, dataOp, this);
			} else {
				dataOp.status(SUCC);
				handleCompleted((O) dataOp);
			}
		} else {
			throw new AssertionError("Invalid load operation status upon invocation: " + dataOp.status());
		}
	}

	@Override @SuppressWarnings("unchecked")
	public void completed(final Integer n, final DataOperation<? extends DataItem> dataOp) {
		final var newCountBytesDone = dataOp.countBytesDone() + n;
		dataOp.countBytesDone(newCountBytesDone);
		dataOp.item().position(newCountBytesDone); // TODO correct only for full create/full read
		// resubmit for the new iteration
		if(!childOpQueue.offer((O) dataOp)) {
			throw new AssertionError("Failed to resubmit the active load operation");
		}
	}

	@Override
	public void failed(final Throwable thrown, final DataOperation<? extends DataItem> dataOp) {
		LogUtil.exception(Level.ERROR, thrown, "{}: data load operation \"{}\" failure", stepId, dataOp);
		dataOp.status(FAIL_IO);
		handleCompleted((O) dataOp);
	}
}
