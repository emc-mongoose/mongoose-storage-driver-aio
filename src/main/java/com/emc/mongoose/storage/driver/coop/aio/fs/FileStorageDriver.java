package com.emc.mongoose.storage.driver.coop.aio.fs;

import com.emc.mongoose.base.config.IllegalConfigurationException;
import com.emc.mongoose.base.data.DataInput;
import com.emc.mongoose.base.item.DataItem;
import com.emc.mongoose.base.item.Item;
import com.emc.mongoose.base.item.ItemFactory;
import com.emc.mongoose.base.item.op.OpType;
import com.emc.mongoose.base.item.op.Operation;
import com.emc.mongoose.base.item.op.data.DataOperation;
import com.emc.mongoose.base.item.op.path.PathOperation;
import com.emc.mongoose.base.logging.LogUtil;
import com.emc.mongoose.base.storage.Credential;
import com.emc.mongoose.storage.driver.coop.CoopStorageDriverBase;
import com.emc.mongoose.storage.driver.coop.aio.AsyncChannel;
import com.github.akurilov.confuse.Config;
import org.apache.logging.log4j.Level;

import java.io.File;
import java.io.IOException;
import java.nio.file.AccessDeniedException;
import java.nio.file.FileSystemException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.emc.mongoose.base.Exceptions.throwUncheckedIfInterrupted;
import static com.emc.mongoose.storage.driver.coop.aio.fs.FsConstants.CREATE_OPEN_OPT;
import static com.emc.mongoose.storage.driver.coop.aio.fs.FsConstants.FS;
import static com.emc.mongoose.storage.driver.coop.aio.fs.FsConstants.FS_PROVIDER;
import static com.emc.mongoose.storage.driver.coop.aio.fs.FsConstants.WRITE_OPEN_OPT;

public class FileStorageDriver<I extends Item, O extends Operation<I>>
extends CoopStorageDriverBase<I, O> {

	private final Map<DataOperation, AsyncChannel> srcOpenChannels = new ConcurrentHashMap<>();
	private final Map<DataOperation, AsyncChannel> dstOpenChannels = new ConcurrentHashMap<>();
	private final Map<String, File> dstParentDirs = new ConcurrentHashMap<>();

	public FileStorageDriver(
		final String testStepId, final DataInput dataInput, final Config storageConfig, final boolean verifyFlag,
		final int batchSize
	) throws IllegalConfigurationException {
		super(testStepId, dataInput, storageConfig, verifyFlag, batchSize);
	}

	protected <F extends DataItem, D extends DataOperation<F>> AsyncChannel openDstChan(final D dataOp) {
		final var fileItemName = dataOp.item().name();
		final var opType = dataOp.type();
		final var dstPath = dataOp.dstPath();
		final Path itemPath;
		try {
			if (dstPath == null || dstPath.isEmpty() || fileItemName.startsWith(dstPath)) {
				itemPath = FS.getPath(fileItemName);
			} else {
				dstParentDirs.computeIfAbsent(dstPath, DirIoHelper::createParentDir);
				itemPath = FS.getPath(dstPath, fileItemName);
			}
			if (OpType.CREATE.equals(opType)) {
				return AsyncChannel.wrap(FS_PROVIDER.newAsynchronousFileChannel(itemPath, CREATE_OPEN_OPT, null));
			} else {
				return AsyncChannel.wrap(FS_PROVIDER.newAsynchronousFileChannel(itemPath, WRITE_OPEN_OPT, null));
			}
		} catch (final AccessDeniedException e) {
			dataOp.status(Operation.Status.RESP_FAIL_AUTH);
			LogUtil.exception(Level.DEBUG, e, "Access denied to open the output channel for the path \"{}\"", dstPath);
		} catch (final NoSuchFileException e) {
			dataOp.status(Operation.Status.FAIL_IO);
			LogUtil.exception(Level.DEBUG, e, "Failed to open the output channel for the path \"{}\"", dstPath);
		} catch (final FileSystemException e) {
			final var freeSpace = (new File(e.getFile())).getFreeSpace();
			if (freeSpace > 0) {
				dataOp.status(Operation.Status.FAIL_IO);
				LogUtil.exception(Level.DEBUG, e, "Failed to open the output channel for the path \"{}\"", dstPath);
			} else {
				dataOp.status(Operation.Status.RESP_FAIL_SPACE);
				LogUtil.exception(Level.DEBUG, e, "No free space for the path \"{}\"", dstPath);
			}
		} catch (final IOException e) {
			dataOp.status(Operation.Status.FAIL_IO);
			LogUtil.exception(Level.DEBUG, e, "Failed to open the output channel for the path \"{}\"", dstPath);
		} catch (final Throwable cause) {
			throwUncheckedIfInterrupted(cause);
			dataOp.status(Operation.Status.FAIL_UNKNOWN);
			LogUtil.exception(Level.WARN, cause, "Failed to open the output channel for the path \"{}\"", dstPath);
		}
		return null;
	}

	@Override
	protected boolean submit(final O op)
	throws IllegalStateException {
		if (op instanceof DataOperation) {
			return submitDataOperation((DataOperation<? extends DataItem>) op);
		} else if (op instanceof PathOperation) {
			throw new AssertionError("Not implemented");
		} else {
			throw new AssertionError("Not implemented");
		}
	}

	final boolean submitDataOperation(final DataOperation<? extends DataItem> op) {

	}

	@Override
	protected int submit(final List<O> ops, final int from, final int to)
	throws IllegalStateException {
		return 0;
	}

	@Override
	protected int submit(final List<O> ops)
	throws IllegalStateException {
		return 0;
	}

	@Override
	protected String requestNewPath(final String path) {
		final File pathFile = FS.getPath(path).toFile();
		if (!pathFile.exists()) {
			pathFile.mkdirs();
		}
		return path;
	}

	@Override
	protected String requestNewAuthToken(final Credential credential) {
		throw new AssertionError("Should not be invoked");
	}

	@Override
	public List<I> list(
		final ItemFactory<I> itemFactory, final String path, final String prefix, final int idRadix,
		final I lastPrevItem, final int count
	) throws IOException {
		return ListingHelper.list(itemFactory, path, prefix, idRadix, lastPrevItem, count);
	}

	@Override
	public void adjustIoBuffers(final long avgTransferSize, final OpType opType) {
	}

	@Override
	protected final void doClose()
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

	@Override
	public final String toString() {
		return String.format(super.toString(), "fs");
	}
}
