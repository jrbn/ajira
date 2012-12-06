package arch.datalayer;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import arch.ActionContext;
import arch.Context;
import arch.chains.Chain;
import arch.chains.ChainLocation;
import arch.data.types.Tuple;
import arch.utils.Configuration;
import arch.utils.Consts;

public abstract class InputLayer {

	static final Logger log = LoggerFactory.getLogger(InputLayer.class);

	static InputLayer storage;

	static public InputLayer getImplementation(Configuration configuration)
			throws IOException, ClassNotFoundException, InstantiationException,
			IllegalAccessException {

		if (storage == null) {
			String className = configuration.get(Consts.STORAGE_IMPL, null);
			if (className == null) {
				throw new IOException(
						"Storage implementation class not defined!");
			}

			Class<? extends InputLayer> cStorage = ClassLoader
					.getSystemClassLoader().loadClass(className)
					.asSubclass(InputLayer.class);
			storage = cStorage.newInstance();
		}

		return storage;
	}

	public void startup(Context context) throws Exception {
		long time = System.currentTimeMillis();
		load(context);
		log.debug("Time to load the storage (ms): "
				+ (System.currentTimeMillis() - time));
	}

	protected abstract void load(Context context) throws Exception;

	public abstract TupleIterator getIterator(Tuple tuple, ActionContext context);

	public abstract void releaseIterator(TupleIterator itr,
			ActionContext context);

	public abstract ChainLocation getLocations(Tuple tuple, Chain chain,
			Context context);
}
