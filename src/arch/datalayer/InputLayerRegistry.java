package arch.datalayer;

import arch.utils.Consts;

public class InputLayerRegistry {
	
	InputLayer[] registry = new InputLayer[Consts.MAX_N_INPUT_LAYERS];

	public InputLayer getInputLayer(int idInputLayer) {
		return registry[idInputLayer];
	}

	public void add(int bucketInputLayerId, InputLayer bucketInput) {
		registry[bucketInputLayerId] = bucketInput;
	}

}
