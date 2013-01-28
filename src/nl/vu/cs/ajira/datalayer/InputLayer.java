package nl.vu.cs.ajira.datalayer;

import java.io.IOException;

import nl.vu.cs.ajira.Context;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.chains.ChainLocation;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.datalayer.files.FileLayer;
import nl.vu.cs.ajira.utils.Configuration;
import nl.vu.cs.ajira.utils.Consts;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class InputLayer {

	static final Logger log = LoggerFactory.getLogger(InputLayer.class);

	static InputLayer storage;

	static public InputLayer getImplementation(Configuration configuration)
			throws IOException, ClassNotFoundException, InstantiationException,
			IllegalAccessException {

		if (storage == null) {
			String className = configuration.get(Consts.STORAGE_IMPL, null);
			if (className == null) {
				log.info("Storage implementation class not defined!. Use FileLayer instead.");
				className = FileLayer.class.getName();
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
		log.debug("Time to load inputLayer " + this.getClass().getName()
				+ " (ms): " + (System.currentTimeMillis() - time));
	}

	protected abstract void load(Context context) throws Exception;

	public abstract TupleIterator getIterator(Tuple tuple, ActionContext context);

	public abstract void releaseIterator(TupleIterator itr,
			ActionContext context);

	public abstract ChainLocation getLocations(Tuple tuple,
			ActionContext context);

	public abstract String getName();
}
