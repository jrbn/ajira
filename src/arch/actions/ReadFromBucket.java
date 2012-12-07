package arch.actions;

import arch.ActionContext;
import arch.chains.Chain;
import arch.data.types.TInt;
import arch.data.types.Tuple;
import arch.storage.container.WritableContainer;
import arch.utils.Consts;

public class ReadFromBucket extends Action {

	int node;
	int bucketId;
	boolean forward = false;

	public static final int BUCKET_ID = 0;
	public static final String S_BUCKET_ID = "bucket_id";
	public static final int NODE_ID = 1;
	public static final String S_NODE_ID = "node_id";
	public static final int FORWARD_TUPLES = 2;
	public static final String S_FORWARD_TUPLES = "forward_tuples";

	static {
		registerParameter(BUCKET_ID, S_BUCKET_ID, null, true);
		registerParameter(NODE_ID, S_NODE_ID, null, true);
		registerParameter(FORWARD_TUPLES, S_FORWARD_TUPLES, false, false);
	}

	@Override
	public void startProcess(ActionContext context, Chain chain)
			throws Exception {
		bucketId = getParamInt(BUCKET_ID);
		node = getParamInt(NODE_ID);
		forward = getParamBoolean(FORWARD_TUPLES);
	}

	@Override
	public void process(ActionContext context, Chain chain, Tuple inputTuple,
			WritableContainer<Tuple> output,
			WritableContainer<Chain> chainsToProcess) throws Exception {
		if (forward)
			output.add(inputTuple);
	}

	@Override
	public void stopProcess(ActionContext context, Chain chain,
			WritableContainer<Tuple> output,
			WritableContainer<Chain> chainsToSend) throws Exception {
		// Generate a new chain and send it.
		Chain newChain = new Chain();
		chain.createBranch(context, newChain);

		newChain.setInputLayerId(Consts.BUCKET_INPUT_LAYER_ID);
		newChain.setInputTuple(new Tuple(new TInt(newChain.getSubmissionId()),
				new TInt(bucketId), new TInt(node)));

		chainsToSend.add(newChain);
	}
}
