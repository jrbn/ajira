package arch.actions;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import arch.ActionContext;
import arch.chains.Chain;
import arch.data.types.Tuple;
import arch.storage.container.WritableContainer;

public class RemoveDuplicates extends Action {

	@Override
	public void readFrom(DataInput input) throws IOException {
	}

	@Override
	public void writeTo(DataOutput output) throws IOException {
	}

	@Override
	public int bytesToStore() {
		return 0;
	}

	private final Tuple tuple = new Tuple();
	boolean first = true;
	long filtered = 0;
	long total = 0;

	@Override
	public void startProcess(ActionContext context, Chain chain)
			throws Exception {
		first = true;
		filtered = total = 0;
	}

	@Override
	public void process(ActionContext context, Chain chain,
			Tuple inputTuple,
			WritableContainer<Tuple> output,
			WritableContainer<Chain> chainsToResolve, WritableContainer<Chain> chainsToProcess)
			throws Exception {

		if (inputTuple == null) {
			if (!first) {
				output.add(tuple);
			}
		} else {
			if (first) {
				inputTuple.copyTo(tuple);
				first = false;
			} else {
				if (tuple.compareTo(inputTuple) != 0) {
					output.add(tuple);
					inputTuple.copyTo(tuple);
				}
			}
		}
	}

	@Override
	public void stopProcess(ActionContext context, Chain chain,
			WritableContainer<Tuple> output,
			WritableContainer<Chain> newChains,
			WritableContainer<Chain> chainsToSend) throws Exception {
		process(context, chain, null, output, null, null);
	}
}
