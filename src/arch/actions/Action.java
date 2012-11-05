package arch.actions;

import arch.ActionContext;
import arch.chains.Chain;
import arch.data.types.Tuple;
import arch.storage.Writable;
import arch.storage.container.WritableContainer;

public abstract class Action extends Writable {

	private Tuple tuple = null;

	public boolean blockProcessing() {
		return false;
	}

	public void setInputTuple(Tuple tuple) {
		this.tuple = tuple;
	}

	public Tuple getInputTuple() {
		return tuple;
	}

	/******
	 * Method called during the execution of the chain
	 * 
	 * @throws Exception
	 ******/
	public void startProcess(ActionContext context, Chain chain)
			throws Exception {
	}

	public abstract void process(Tuple inputTuple, Chain remainingChain,
			WritableContainer<Chain> chainsToResolve,
			WritableContainer<Chain> chainsToProcess,
			WritableContainer<Tuple> output, ActionContext context)
			throws Exception;

	public void stopProcess(ActionContext context, Chain chain,
			WritableContainer<Tuple> output,
			WritableContainer<Chain> newChains,
			WritableContainer<Chain> chainsToSend) throws Exception {
	}

}