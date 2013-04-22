package nl.vu.cs.ajira.datalayer;

import java.util.HashMap;
import java.util.Map;

import nl.vu.cs.ajira.Context;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is used to keep track of the InputLayers that are used. Provides a
 * method to load all the layers.
 * 
 */
public class InputLayerRegistry {

	private static final Logger log = LoggerFactory
			.getLogger(InputLayerRegistry.class);

	private final Map<Class<? extends InputLayer>, InputLayer> registry = new HashMap<Class<? extends InputLayer>, InputLayer>();
	private InputLayer def;

	/**
	 * 
	 * @param idInputLayer
	 *            The id of the InputLayer that is looked.
	 * @return The InputLayer with the id idInputLayer.
	 * @throws IllegalAccessException
	 * @throws InstantiationException
	 */
	public InputLayer getLayer(Class<? extends InputLayer> clazz) {
		if (clazz == InputLayer.DEFAULT_LAYER) {
			return def;
		}
		InputLayer il = registry.get(clazz);
		if (il == null) {
			try {
				il = clazz.newInstance();
				registerLayer(il, false);
			} catch (Exception e) {
				log.error("error", e);
			}
		}
		return il;
	}

	public void registerLayer(InputLayer input, boolean isDefault) {
		try {
			registry.put(input.getClass(), input);
			if (isDefault) {
				def = input;
			}
		} catch (Exception e) {
			log.error("error", e);
		}
	}

	/**
	 * 
	 * Loads each InputLayer found in the registry for the global context.
	 * 
	 * @param globalContext
	 *            The global context.
	 * @throws Exception
	 */
	public void startup(Context globalContext) throws Exception {
		for (InputLayer i : registry.values()) {
			if (i != null) {
				i.startup(globalContext);
			}
		}
	}

	/**
	 * Closes each InputLayer found in the registry.
	 */
	public void close() {
		for (InputLayer i : registry.values()) {
			if (i != null) {
				i.close();
			}
		}
	}

}
