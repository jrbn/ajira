package nl.vu.cs.ajira.datalayer;

import nl.vu.cs.ajira.Context;
import nl.vu.cs.ajira.utils.Consts;

public class InputLayerRegistry {
	
	InputLayer[] registry = new InputLayer[Consts.MAX_N_INPUT_LAYERS];

	public InputLayer getInputLayer(int idInputLayer) {
		return registry[idInputLayer];
	}

	public void add(int bucketInputLayerId, InputLayer bucketInput) {
		registry[bucketInputLayerId] = bucketInput;
	}

	public void startup(Context globalContext) throws Exception {
		for (InputLayer i : registry) {
			if (i != null) {
				i.startup(globalContext);
			}
		}
	}

}
