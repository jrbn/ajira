package arch.actions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import arch.ActionContext;
import arch.actions.support.HashPartitioner;
import arch.actions.support.Partitioner;
import arch.buckets.Bucket;
import arch.buckets.Buckets;
import arch.chains.Chain;
import arch.data.types.TInt;
import arch.data.types.Tuple;
import arch.storage.container.WritableContainer;
import arch.utils.Consts;

public class DistributeTuples extends Action {

	public static final int MULTIPLE = -1;
	public static final int ALL = -2;

	/* PARAMETERS */
	public static final int SORTING_FUNCTION = 0;
	public static final String S_SORTING_FUNCTION = "sorting_function";
	public static final int PARTITIONER = 1;
	public static final String S_PARTITIONER = "partitioner";
	private static final int BUCKET_ID = 2;
	private static final String S_BUCKET_ID = "bucket_id";

	static final Logger log = LoggerFactory.getLogger(DistributeTuples.class);

	private int bucketId = -1;
	private String sortingFunction = null;
	private Bucket[] bucketsCache;
	private int nPartitions;
	private String sPartitioner = null;
	private Partitioner partitioner = null;

	static class ParametersProcessor extends
			ActionConf.RuntimeParameterProcessor {
		@Override
		public void processParameters(Chain chain, Object[] params,
				ActionContext context) {
			if (params[BUCKET_ID] == null) {
				params[BUCKET_ID] = context.getNewBucketID();
			}
		}
	}

	@Override
	public boolean blockProcessing() {
		return true;
	}

	@Override
	public void setupActionParameters(ActionConf conf) throws Exception {
		conf.registerParameter(SORTING_FUNCTION, S_SORTING_FUNCTION, null,
				false);
		conf.registerParameter(PARTITIONER, S_PARTITIONER,
				HashPartitioner.class.getName(), false);
		conf.registerParameter(BUCKET_ID, S_BUCKET_ID, null, true);
		conf.registerRuntimeParameterProcessor(ParametersProcessor.class);
	}

	@Override
	public void startProcess(ActionContext context, Chain chain)
			throws Exception {
		sortingFunction = getParamString(SORTING_FUNCTION);
		sPartitioner = getParamString(PARTITIONER);

		// Init variables
		nPartitions = context.getNetworkLayer().getNumberNodes();
		bucketsCache = new Bucket[context.getNetworkLayer().getNumberNodes()];
		partitioner = null;
	}

	@Override
	public void process(ActionContext context, Chain chain, Tuple inputTuple,
			WritableContainer<Tuple> outputTuples,
			WritableContainer<Chain> chainsToProcess) {
		try {

			// First partition the data
			if (partitioner == null) {
				partitioner = (Partitioner) Class.forName(sPartitioner)
						.newInstance();
				partitioner.init(context);
			}

			int nodeId = partitioner.partition(inputTuple, nPartitions);

			Bucket b = bucketsCache[nodeId];
			if (b == null) {
				b = context.getTuplesBuckets().startTransfer(
						chain.getSubmissionNode(), chain.getSubmissionId(),
						nodeId % nPartitions, bucketId, sortingFunction, null);
				bucketsCache[nodeId] = b;
			}
			b.add(inputTuple);
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
			int submissionNode = chain.getSubmissionNode();
			long chainId = chain.getChainId();
			long parentChainId = chain.getParentChainId();
			int nchildren = chain.getChainChildren();

			if (context.isCurrentChainRoot() && replicatedFactor > 0) {
				/*** AT FIRST SEND THE CHAINS ***/
				Chain newChain = new Chain();
				chain.copyTo(newChain);
				newChain.setChainChildren(0);
				newChain.setReplicatedFactor(1);
				newChain.setInputLayerId(Consts.BUCKET_INPUT_LAYER_ID);
				newChain.setInputTuple(new Tuple(new TInt(idSubmission),
						new TInt(-1), new TInt(bucketId)));
				chainsToSend.add(newChain);
			}

			Buckets buckets = context.getTuplesBuckets();
			for (int i = 0; i < bucketsCache.length; ++i) {
				buckets.finishTransfer(submissionNode, idSubmission, i,
						this.bucketId, chainId, parentChainId, nchildren,
						replicatedFactor, context.isCurrentChainRoot(),
						sortingFunction, null, bucketsCache[i] != null);
			}
		} catch (Exception e) {
			log.error("Error", e);
		}
		bucketsCache = null;
		partitioner = null;
	}
}
