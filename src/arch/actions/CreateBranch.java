package arch.actions;

import arch.ActionContext;
import arch.chains.Chain;
import arch.data.types.Tuple;
import arch.storage.container.WritableContainer;
import arch.utils.Consts;

public class CreateBranch extends Action {

	/***** PARAMETERS *****/
	public static final int INPUT_LAYER = 0;
	public static final String S_INPUT_LAYER = "input_layer";

	Chain newChain = new Chain();
	int inputLayer;

	@Override
	public void setupActionParameters(ActionConf conf) throws Exception {
		conf.registerParameter(INPUT_LAYER, S_INPUT_LAYER,
				Consts.DEFAULT_INPUT_LAYER_ID, false);
	}

	@Override
	public void startProcess(ActionContext context, Chain chain)
			throws Exception {
		inputLayer = getParamInt(INPUT_LAYER);
	}

	@Override
	public void process(ActionContext context, Chain chain, Tuple inputTuple,
			WritableContainer<Tuple> output,
			WritableContainer<Chain> chainsToProcess) throws Exception {
	}

	@Override
	public void stopProcess(ActionContext context, Chain chain,
			WritableContainer<Tuple> output,
			WritableContainer<Chain> chainsToSend) throws Exception {
		chain.createBranch(context, newChain);
		newChain.replaceInputTuple(null); // Force to read the input of the
											// first action
		newChain.setInputLayerId(inputLayer);
		chainsToSend.add(newChain);
	}
}
