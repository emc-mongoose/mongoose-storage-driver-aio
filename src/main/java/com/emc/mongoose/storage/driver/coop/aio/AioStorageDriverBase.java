package com.emc.mongoose.storage.driver.coop.aio;

import com.emc.mongoose.base.config.IllegalConfigurationException;
import com.emc.mongoose.base.data.DataCorruptionException;
import com.emc.mongoose.base.data.DataInput;
import com.emc.mongoose.base.data.DataSizeException;
import com.emc.mongoose.base.item.DataItem;
import com.emc.mongoose.base.item.Item;
import com.emc.mongoose.base.item.op.Operation;
import com.emc.mongoose.base.item.op.data.DataOperation;
import com.emc.mongoose.storage.driver.coop.CoopStorageDriverBase;
import com.github.akurilov.commons.system.DirectMemUtil;
import com.github.akurilov.confuse.Config;

import java.io.IOException;
import java.nio.ByteBuffer;

import static com.emc.mongoose.base.item.DataItem.rangeOffset;

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
			// TODO replace this by the writeToAsyncByteChannel/writeToAsyncFileChannel method
			countBytesDone += fileItem.writeToFileChannel(dstChannel, contentSize - countBytesDone);
			op.countBytesDone(countBytesDone);
		}
		return countBytesDone >= contentSize;
	}

	static <I extends DataItem, O extends DataOperation<I>> boolean invokeCopy(
		final I fileItem, final O op, final AsyncChannel srcChannel, final AsyncChannel dstChannel) throws IOException {
		long countBytesDone = op.countBytesDone();
		final long contentSize = fileItem.size();
		if (countBytesDone < contentSize && Operation.Status.ACTIVE.equals(op.status())) {
			// TODO something
			countBytesDone += srcChannel.transferTo(
				countBytesDone, contentSize - countBytesDone, dstChannel);
			op.countBytesDone(countBytesDone);
		}
		return countBytesDone >= contentSize;
	}

	static <I extends DataItem, O extends DataOperation<I>> boolean invokeReadAndVerify(
		final I fileItem, final O op, final AsyncChannel srcChannel
	) throws DataSizeException, DataCorruptionException, IOException {
		long countBytesDone = op.countBytesDone();
		final long contentSize = fileItem.size();
		if (countBytesDone < contentSize) {
			if (fileItem.isUpdated()) {
				final DataItem currRange = op.currRange();
				final int nextRangeIdx = op.currRangeIdx() + 1;
				final long nextRangeOffset = rangeOffset(nextRangeIdx);
				if (currRange != null) {
					final ByteBuffer inBuff = DirectMemUtil.getThreadLocalReusableBuff(
						nextRangeOffset - countBytesDone);
					final int n = srcChannel.read(inBuff);
					if (n < 0) {
						throw new DataSizeException(contentSize, countBytesDone);
					} else {
						inBuff.flip();
						currRange.verify(inBuff);
						currRange.position(currRange.position() + n);
						countBytesDone += n;
						if (countBytesDone == nextRangeOffset) {
							op.currRangeIdx(nextRangeIdx);
						}
					}
				} else {
					throw new AssertionError("Null data range");
				}
			} else {
				final ByteBuffer inBuff = DirectMemUtil.getThreadLocalReusableBuff(contentSize - countBytesDone);
				final int n = srcChannel.read(inBuff);
				if (n < 0) {
					throw new DataSizeException(contentSize, countBytesDone);
				} else {
					inBuff.flip();
					fileItem.verify(inBuff);
					fileItem.position(fileItem.position() + n);
					countBytesDone += n;
				}
			}
			op.countBytesDone(countBytesDone);
		}
		return countBytesDone >= contentSize;
	}
}
