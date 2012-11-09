package arch.actions.partitioners;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import arch.ActionContext;
import arch.actions.Action;
import arch.chains.Chain;
import arch.data.types.TInt;
import arch.data.types.Tuple;
import arch.storage.container.WritableContainer;

abstract public class Partitioner extends Action {

	static final Logger log = LoggerFactory.getLogger(Partitioner.class);

	int partitions = -1;

	Tuple tuple = new Tuple();
	TInt tnode = new TInt();

	public void setNumberPartitions(int partitions) {
		this.partitions = partitions;
	}

	@Override
	public void readFrom(DataInput input) throws IOException {
		partitions = input.readInt();
	}

	@Override
	public void writeTo(DataOutput output) throws IOException {
		output.writeInt(partitions);
	}

	@Override
	public int bytesToStore() {
		return 4;
	}

	protected abstract int partition(Tuple tuple, int nnodes) throws Exception;

	@Override
	public void startProcess(ActionContext context, Chain chain)
			throws Exception {
		if (partitions == -1) {
			partitions = context.getNetworkLayer().getNumberNodes();
		}
		tnode.setValue(partitions);
	}

	@Override
	public void process(Tuple inputTuple, Chain remainingChain,
			WritableContainer<Chain> chainsToResolve,
			WritableContainer<Chain> chainsToProcess,
			WritableContainer<Tuple> output, ActionContext context)
			throws Exception {
		inputTuple.copyTo(tuple);

		tnode.setValue(partition(inputTuple, partitions));
		tuple.add(tnode);
		output.add(tuple);
	}
}
