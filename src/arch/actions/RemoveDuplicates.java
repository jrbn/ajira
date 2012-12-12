package arch.actions;

import arch.ActionContext;
import arch.chains.Chain;
import arch.data.types.Tuple;
import arch.storage.container.WritableContainer;

public class RemoveDuplicates extends Action {

	private final Tuple tuple = new Tuple();
	boolean first = true;

	@Override
	public void startProcess(ActionContext context, Chain chain)
			throws Exception {
		first = true;
	}

	@Override
	public void process(ActionContext context, Chain chain, Tuple inputTuple,
			WritableContainer<Tuple> output,
			WritableContainer<Chain> chainsToProcess) throws Exception {
		if (first) {
			inputTuple.copyTo(tuple);
			output.add(inputTuple);
			first = false;
		} else if (inputTuple.compareTo(tuple) != 0) {
			inputTuple.copyTo(tuple);
			output.add(inputTuple);
		}
	}
}
