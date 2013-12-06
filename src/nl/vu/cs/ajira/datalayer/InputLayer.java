package nl.vu.cs.ajira.datalayer;

import nl.vu.cs.ajira.Context;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.chains.Location;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.datalayer.files.FileLayer;
import nl.vu.cs.ajira.utils.Configuration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class InputLayer {

	private static final Logger log = LoggerFactory.getLogger(InputLayer.class);
	public static final String INPUT_LAYER_CLASS = "inputlayer.default";

	public static final Class<? extends InputLayer> DEFAULT_LAYER = InputLayer.class;

	static public Class<? extends InputLayer> getDefaultInputLayerClass(
			Configuration configuration) throws Exception {
		String className = configuration.get(INPUT_LAYER_CLASS, null);
		if (className == null) {
			if (log.isDebugEnabled()) {
				log.debug("Input Layer not specified. Use FileLayer ...");
			}
			className = FileLayer.class.getName();
		}
		return Class.forName(className).asSubclass(InputLayer.class);
	}

	static public void setDefaultInputLayerClass(
			Class<? extends InputLayer> clazz, Configuration configuration) {
		configuration.set(INPUT_LAYER_CLASS, clazz.getName());
	}

	/**
	 * Loads the InputLayer for the current context and logs the time.
	 * 
	 * @param context
	 *            Current context
	 * @throws Exception
	 */
	public void startup(Context context) throws Exception {
		long time = System.currentTimeMillis();
		load(context);
		if (log.isDebugEnabled()) {
			log.debug("Time to load inputLayer " + this.getClass().getName()
					+ " (ms): " + (System.currentTimeMillis() - time));
		}
	}

	/**
	 * Loads the InputLayer for the current context.
	 * 
	 * @param context
	 *            Current context
	 * @throws Exception
	 */
	protected abstract void load(Context context) throws Exception;

	public abstract TupleIterator getIterator(Tuple tuple, ActionContext context)
			throws Exception;

	public abstract void releaseIterator(TupleIterator itr,
			ActionContext context);

	public abstract Location getLocations(Tuple tuple, ActionContext context);

	public void close() {
		// Default implementation is empty.
	}
}
