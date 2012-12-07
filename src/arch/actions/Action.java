package arch.actions;

import arch.ActionContext;
import arch.chains.Chain;
import arch.data.types.Tuple;
import arch.storage.container.WritableContainer;

public abstract class Action extends ActionConf {

	public boolean blockProcessing() {
		return false;
	}

	public void startProcess(ActionContext context, Chain chain)
			throws Exception {
	}

	public abstract void process(ActionContext context, Chain chain,
			Tuple inputTuple, WritableContainer<Tuple> output,
			WritableContainer<Chain> chainsToProcess) throws Exception;

	public void stopProcess(ActionContext context, Chain chain,
			WritableContainer<Tuple> output,
			WritableContainer<Chain> chainsToProcess) throws Exception {
	}
}