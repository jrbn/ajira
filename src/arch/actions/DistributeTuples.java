package arch.actions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import arch.ActionContext;
import arch.actions.support.HashPartitioner;
import arch.actions.support.Partitioner;
import arch.buckets.Bucket;
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
	public static final int NPARTITIONS_PER_NODE = 2;
	public static final String S_NPARTITIONS_PER_NODE = "npartitions_per_node";
	public static final int BUCKET_IDS = 3;
	private static final String S_BUCKET_IDS = "bucket_ids";

	static final Logger log = LoggerFactory.getLogger(DistributeTuples.class);

	private String sortingFunction = null;
	private Bucket[] bucketsCache;
	private int nPartitionsPerNode;
	private String sPartitioner = null;
	private Partitioner partitioner = null;
	private int nPartitions;
	private int[] bucketIds;

	class ParametersProcessor extends ActionConf.RuntimeParameterProcessor {
		@Override
		public void processParameters(Chain chain, Object[] params,
				ActionContext context) {
			if (log.isDebugEnabled()) {
				log.debug("DistributeTuples.processParameters() called");
			}
			// TODO: Should processParameters be allowed to throw an exception?
			// For instance, for inconsistencies.
			if (params[NPARTITIONS_PER_NODE] == null) {
				if (params[BUCKET_IDS] == null) {
					params[NPARTITIONS_PER_NODE] = 1;
				} else {
					Tuple t = (Tuple) params[BUCKET_IDS];
					params[NPARTITIONS_PER_NODE] = t.getNElements();
				}
			}
			if (params[BUCKET_IDS] == null) {
				int np = (Integer) params[NPARTITIONS_PER_NODE];
				Tuple p = new Tuple();
				TInt v = new TInt();
				for (int i = 0; i < np; i++) {
					v.setValue(context.getNewBucketID());
					try {
						p.add(v);
					} catch (Exception e) {
						log.error("Oops: could not add bucket id for partition");
						throw new Error("Could not add bucket id to tuple");
					}
				}
				params[BUCKET_IDS] = p;
			}
			Tuple t = (Tuple) params[BUCKET_IDS];
			if (t.getNElements() != (Integer) params[NPARTITIONS_PER_NODE]) {
				log.error("Oops: inconsistency in number of partitions");
				throw new Error("inconsistency in number of partitions");
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
		conf.registerParameter(NPARTITIONS_PER_NODE, S_NPARTITIONS_PER_NODE,
				null, false);
		conf.registerParameter(BUCKET_IDS, S_BUCKET_IDS, null, false);
		conf.registerRuntimeParameterProcessor(ParametersProcessor.class);
	}

	@Override
	public void startProcess(ActionContext context, Chain chain)
			throws Exception {
		sortingFunction = getParamString(SORTING_FUNCTION);
		sPartitioner = getParamString(PARTITIONER);
		nPartitionsPerNode = getParamInt(NPARTITIONS_PER_NODE);
		Tuple buckets = new Tuple();
		getParamWritable(buckets, BUCKET_IDS);
		TInt v = new TInt();
		bucketIds = new int[buckets.getNElements()];
		for (int i = 0; i < bucketIds.length; i++) {
			buckets.get(v, i);
			bucketIds[i] = v.getValue();
		}
		nPartitions = nPartitionsPerNode * context.getNumberNodes();

		// Init variables
		bucketsCache = new Bucket[nPartitions];
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

			int partition = partitioner.partition(inputTuple, nPartitions);

			Bucket b = bucketsCache[partition];
			if (b == null) {
				int nodeNo = partition / nPartitionsPerNode;
				int bucketNo = bucketIds[partition % nPartitionsPerNode];
				b = context.startTransfer(nodeNo, bucketNo, sortingFunction);
				bucketsCache[partition] = b;
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

			for (int i = 0; i < nPartitionsPerNode; i++) {
				if (context.isCurrentChainRoot() && replicatedFactor > 0) {
					/*** AT FIRST SEND THE CHAINS ***/
					Chain newChain = new Chain();
					chain.copyTo(newChain);
					newChain.setChainChildren(0);
					newChain.setReplicatedFactor(1);
					newChain.setInputLayerId(Consts.BUCKET_INPUT_LAYER_ID);
					newChain.setInputTuple(new Tuple(new TInt(chain
							.getSubmissionId()), new TInt(bucketIds[i]),
							new TInt(-1)));
					chainsToSend.add(newChain);
				}
			}

			for (int i = 0; i < nPartitions; ++i) {
				int nodeNo = i / nPartitionsPerNode;
				context.finishTransfer(nodeNo, bucketIds[i], sortingFunction,
						bucketsCache[i] != null);
			}
		} catch (Exception e) {
			log.error("Error", e);
		}
		bucketsCache = null;
		partitioner = null;
	}
}
