package com.emc.mongoose.storage.driver.coop.aio;

import com.emc.mongoose.base.config.IllegalConfigurationException;
import com.emc.mongoose.base.data.DataInput;
import com.emc.mongoose.base.item.DataItem;
import com.emc.mongoose.base.item.Item;
import com.emc.mongoose.base.item.op.Operation;
import com.emc.mongoose.base.item.op.data.DataOperation;
import com.emc.mongoose.storage.driver.coop.CoopStorageDriverBase;
import com.github.akurilov.confuse.Config;

import java.io.IOException;

public abstract class AioStorageDriverBase<I extends Item, O extends Operation<I>>
extends CoopStorageDriverBase<I, O> {

	protected AioStorageDriverBase(
		final String testStepId, final DataInput dataInput, final Config storageConfig, final boolean verifyFlag,
		final int batchSize
	) throws IllegalConfigurationException {
		super(testStepId, dataInput, storageConfig, verifyFlag, batchSize);
	}

	protected abstract <F extends DataItem, D extends DataOperation<F>> AsyncChannel openDstChan(final D dataOp);

	static <I extends DataItem, O extends DataOperation<I>> boolean invokeCreate(
		final I fileItem, final O op, final AsyncChannel dstChannel) throws IOException {
		long countBytesDone = op.countBytesDone();
		final long contentSize = fileItem.size();
		if (countBytesDone < contentSize && Operation.Status.ACTIVE.equals(op.status())) {
			countBytesDone += fileItem.writeToFileChannel(dstChannel, contentSize - countBytesDone);
			op.countBytesDone(countBytesDone);
		}
		return countBytesDone >= contentSize;
	}
}
