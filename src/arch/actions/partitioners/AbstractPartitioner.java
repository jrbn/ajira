package arch.actions.partitioners;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import arch.ActionContext;
import arch.actions.Action;
import arch.actions.ActionConf;
import arch.chains.Chain;
import arch.data.types.TInt;
import arch.data.types.Tuple;
import arch.storage.container.WritableContainer;

abstract public class AbstractPartitioner extends Action {

	static final Logger log = LoggerFactory
			.getLogger(AbstractPartitioner.class);

	public static final int N_PARTITIONS = 0;
	public static final String S_N_PARTITIONS = "n_partitions";

	int partitions = -1;

	Tuple tuple = new Tuple();
	TInt tnode = new TInt();

	protected abstract int partition(Tuple tuple, int nnodes) throws Exception;

	@Override
	public final void setupActionParameters(ActionConf conf) throws Exception {
		conf.registerParameter(N_PARTITIONS, S_N_PARTITIONS, -1, false);
	}

	@Override
	public void startProcess(ActionContext context, Chain chain)
			throws Exception {
		partitions = getParamInt(N_PARTITIONS);
		if (partitions == -1) {
			partitions = context.getNetworkLayer().getNumberNodes();
		}
		tnode.setValue(partitions);
	}

	@Override
	public void process(ActionContext context, Chain chain, Tuple inputTuple,
			WritableContainer<Tuple> output,
			WritableContainer<Chain> chainsToProcess) throws Exception {
		inputTuple.copyTo(inputTuple);

		tnode.setValue(partition(inputTuple, partitions));
		inputTuple.add(tnode);
		output.add(inputTuple);
	}
}