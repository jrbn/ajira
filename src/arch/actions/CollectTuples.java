package arch.actions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import arch.ActionContext;
import arch.buckets.Bucket;
import arch.chains.Chain;
import arch.data.types.TInt;
import arch.data.types.Tuple;
import arch.storage.container.WritableContainer;
import arch.utils.Consts;

public class CollectTuples extends Action {

	static final Logger log = LoggerFactory.getLogger(CollectTuples.class);

	/* PARAMETERS */
	public static final int NODE_ID = 0;
	public static final String S_NODE_ID = "node_id";
	private static final int BUCKET_ID = 1;
	private static final String S_BUCKET_ID = "bucket_id";
	public static final int SORTING_FUNCTION = 2;
	public static final String S_SORTING_FUNCTION = "sorting_function";

	private int nodeId;
	private int bucketId = -1;
	private String sortingFunction = null;
	private Bucket bucket;

	@Override
	public boolean blockProcessing() {
		return true;
	}

	static class ParametersProcessor extends
			ActionConf.RuntimeParameterProcessor {
		@Override
		public void processParameters(Chain chain, Object[] params,
				ActionContext context) {
			if (params[NODE_ID] == null) {
				params[NODE_ID] = context.getMyNodeId();
			}
			if (params[BUCKET_ID] == null) {
				params[BUCKET_ID] = context.getNewBucketID();
			}
		}
	}

	@Override
	public void setupActionParameters(ActionConf conf) throws Exception {
		conf.registerParameter(NODE_ID, S_NODE_ID, null, true);
		conf.registerParameter(BUCKET_ID, S_BUCKET_ID, null, true);
		conf.registerParameter(SORTING_FUNCTION, S_SORTING_FUNCTION, null,
				false);
		conf.registerRuntimeParameterProcessor(ParametersProcessor.class);
	}

	@Override
	public void startProcess(ActionContext context, Chain chain)
			throws Exception {
		nodeId = getParamInt(NODE_ID);
		sortingFunction = getParamString(SORTING_FUNCTION);
		bucket = null;
	}

	@Override
	public void process(ActionContext context, Chain chain, Tuple inputTuple,
			WritableContainer<Tuple> outputTuples,
			WritableContainer<Chain> chainsToProcess) {
		try {
			if (bucket == null) {
				bucket = context.startTransfer(nodeId, bucketId,
						sortingFunction);
			}
			bucket.add(inputTuple);
		} catch (Exception e) {
			log.error("Failed processing tuple. Chain=" + chain.toString(), e);
		}
	}

	@Override
	public void stopProcess(ActionContext context, Chain chain,
			WritableContainer<Tuple> outputTuples,
			WritableContainer<Chain> chainsToSend) {
		try {
			// Send the chains to process the buckets to all the nodes that
			// will host the buckets
			int replicatedFactor = chain.getReplicatedFactor();
			int idSubmission = chain.getSubmissionId();
			if (context.isCurrentChainRoot() && replicatedFactor > 0) {
				/*** AT FIRST SEND THE CHAINS ***/
				Chain newChain = new Chain();
				chain.copyTo(newChain);
				newChain.setChainChildren(0);
				newChain.setReplicatedFactor(1);
				newChain.setInputLayerId(Consts.BUCKET_INPUT_LAYER_ID);
				newChain.setInputTuple(new Tuple(new TInt(idSubmission),
						new TInt(bucketId), new TInt(nodeId)));
				chainsToSend.add(newChain);
			}

			context.finishTransfer(nodeId, bucketId, sortingFunction,
					bucket != null);

		} catch (Exception e) {
			log.error("Error", e);
		}
		bucket = null;
	}
}
