package com.emc.mongoose.storage.driver.coop.aio.mock;

import com.emc.mongoose.base.config.IllegalConfigurationException;
import com.emc.mongoose.base.data.DataInput;
import com.emc.mongoose.base.env.ExtensionBase;
import com.emc.mongoose.base.item.Item;
import com.emc.mongoose.base.item.op.Operation;
import com.emc.mongoose.base.storage.driver.StorageDriverFactory;
import com.github.akurilov.confuse.Config;
import com.github.akurilov.confuse.SchemaProvider;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class AioStorageDriverMockExtension<I extends Item, O extends Operation<I>, T extends AioStorageDriverMock<I, O>>
	extends ExtensionBase
	implements StorageDriverFactory<I, O, T> {
	private static final List<String> RES_INSTALL_FILES = Collections.unmodifiableList(
		Arrays.asList());

	@Override
	public final String id() {
		return "aio-mock";
	}

	@Override
	@SuppressWarnings("unchecked")
	public T create(
		final String stepId, final DataInput dataInput, final Config storageConfig, final boolean verifyFlag,
		final int batchSize) throws IllegalConfigurationException, InterruptedException {
		return (T) new AioStorageDriverMock<I, O>(stepId, dataInput, storageConfig, verifyFlag, batchSize);
	}

	@Override
	public final SchemaProvider schemaProvider() {
		return null;
	}

	@Override
	protected final String defaultsFileName() {
		return null;
	}

	@Override
	protected final List<String> resourceFilesToInstall() {
		return RES_INSTALL_FILES;
	}

}
