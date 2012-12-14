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
	boolean branch = false;

	public static final int BUCKET_ID = 0;
	public static final String S_BUCKET_ID = "bucket_id";
	public static final int NODE_ID = 1;
	public static final String S_NODE_ID = "node_id";
	public static final int BRANCH = 2;
	public static final String S_BRANCH = "branch";

	static class ParametersProcessor extends
			ActionConf.RuntimeParameterProcessor {
		@Override
		void processParameters(Chain chain, Object[] params,
				ActionContext context) {
			if ((Boolean) params[BRANCH] == false) {
				chain.setInputLayerId(Consts.BUCKET_INPUT_LAYER_ID);
				chain.setInputTuple(new Tuple(
						new TInt(chain.getSubmissionId()), new TInt(
								(Integer) params[BUCKET_ID]), new TInt(
								(Integer) params[NODE_ID])));
			}
		}
	}

	@Override
	public void setupActionParameters(ActionConf conf) throws Exception {
		conf.registerParameter(BUCKET_ID, S_BUCKET_ID, null, true);
		conf.registerParameter(NODE_ID, S_NODE_ID, -1, false);
		conf.registerParameter(BRANCH, S_BRANCH, true, false);
	}

	@Override
	public void startProcess(ActionContext context, Chain chain)
			throws Exception {
		bucketId = getParamInt(BUCKET_ID);
		node = getParamInt(NODE_ID);
		branch = getParamBoolean(BRANCH);
	}

	@Override
	public void process(ActionContext context, Chain chain, Tuple inputTuple,
			WritableContainer<Tuple> output,
			WritableContainer<Chain> chainsToProcess) throws Exception {
		if (!branch)
			output.add(inputTuple);
	}

	@Override
	public void stopProcess(ActionContext context, Chain chain,
			WritableContainer<Tuple> output,
			WritableContainer<Chain> chainsToSend) throws Exception {
		if (branch) {
			Chain newChain = new Chain();
			chain.branch(newChain);
			newChain.setInputLayerId(Consts.BUCKET_INPUT_LAYER_ID);
			newChain.setInputTuple(new Tuple(new TInt(newChain
					.getSubmissionId()), new TInt(bucketId), new TInt(node)));
			chainsToSend.add(newChain);
		}
	}
}
