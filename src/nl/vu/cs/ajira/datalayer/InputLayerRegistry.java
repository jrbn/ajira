package nl.vu.cs.ajira.datalayer;

import nl.vu.cs.ajira.Context;
import nl.vu.cs.ajira.utils.Consts;

/**
 * This class is used to keep track of the InputLayers 
 * that are used. Provides a method to load all the layers.
 *
 */
public class InputLayerRegistry {
	
	InputLayer[] registry = new InputLayer[Consts.MAX_N_INPUT_LAYERS];

	/**
	 * 
	 * @param idInputLayer
	 * 		The id of the InputLayer that is looked.
	 * @return
	 * 		The InputLayer with the id idInputLayer.
	 */
	public InputLayer getInputLayer(int idInputLayer) {
		return registry[idInputLayer];
	}

	/**
	 * Adds to the registry the InputLayer bucketInput
	 * on the position bucketInputLayerId.
	 * 
	 * @param bucketInputLayerId
	 * 		The id of the InputLayer that is added to the registry.
	 * @param bucketInput
	 * 		The inputLayer that is added to registry.
	 */
	public void add(int bucketInputLayerId, InputLayer bucketInput) {
		registry[bucketInputLayerId] = bucketInput;
	}

	/**
	 * 
	 * Loads each InputLayer found in the registry
	 * for the global context.
	 * 
	 * @param globalContext
	 * 		The global context.
	 * @throws Exception
	 */
	public void startup(Context globalContext) throws Exception {
		for (InputLayer i : registry) {
			if (i != null) {
				i.startup(globalContext);
			}
		}
	}

	/**
	 * Closes each InputLayer found in the registry.
	 */
	public void close() {
		for (InputLayer i : registry) {
			if (i != null) {
				i.close();
			}
		}
	}

}
