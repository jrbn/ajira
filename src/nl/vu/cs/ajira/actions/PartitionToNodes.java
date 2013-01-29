package nl.vu.cs.ajira.actions;

import nl.vu.cs.ajira.actions.support.HashPartitioner;
import nl.vu.cs.ajira.actions.support.Partitioner;
import nl.vu.cs.ajira.buckets.Bucket;
import nl.vu.cs.ajira.data.types.TInt;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.data.types.TupleFactory;
import nl.vu.cs.ajira.datalayer.Query;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PartitionToNodes extends Action {

	/* PARAMETERS */
	public static final int SORT = 0;
	public static final String S_SORT = "sort";
	public static final int PARTITIONER = 1;
	public static final String S_PARTITIONER = "partitioner";
	public static final int NPARTITIONS_PER_NODE = 2;
	public static final String S_NPARTITIONS_PER_NODE = "npartitions_per_node";
	private static final int BUCKET_IDS = 3;
	private static final String S_BUCKET_IDS = "bucket_ids";
	public static final int SORTING_FIELDS = 4;
	public static final String S_SORTING_FIELDS = "sorting_fields";

	static final Logger log = LoggerFactory.getLogger(PartitionToNodes.class);

	private boolean shouldSort;
	private byte[] sortingFields = null;

	private Bucket[] bucketsCache;
	private int nPartitionsPerNode;
	private String sPartitioner = null;
	private Partitioner partitioner = null;
	private int nPartitions;
	private int[] bucketIds;

	public static class ParametersProcessor extends ActionConf.Configurator {
		@Override
		public void setupConfiguration(Query query, Object[] params,
				ActionController controller, ActionContext context) {
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
				Tuple p = TupleFactory.newTuple();
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

			controller.continueComputationOn(-1, ((TInt) t.get(0)).getValue());
		}
	}

	@Override
	public void registerActionParameters(ActionConf conf) throws Exception {
		conf.registerParameter(SORT, S_SORT, false, false);
		conf.registerParameter(PARTITIONER, S_PARTITIONER,
				HashPartitioner.class.getName(), false);
		conf.registerParameter(NPARTITIONS_PER_NODE, S_NPARTITIONS_PER_NODE,
				null, false);
		conf.registerParameter(BUCKET_IDS, S_BUCKET_IDS, null, false);
		conf.registerParameter(SORTING_FIELDS, S_SORTING_FIELDS, null, false);

		conf.registerCustomConfigurator(ParametersProcessor.class);
	}

	@Override
	public void startProcess(ActionContext context) throws Exception {
		shouldSort = getParamBoolean(SORT);
		sortingFields = getParamByteArray(SORTING_FIELDS);

		sPartitioner = getParamString(PARTITIONER);
		nPartitionsPerNode = getParamInt(NPARTITIONS_PER_NODE);

		Tuple t = TupleFactory.newTuple();
		getParamWritable(t, BUCKET_IDS);
		bucketIds = new int[t.getNElements()];
		for (int i = 0; i < bucketIds.length; i++) {
			bucketIds[i] = ((TInt) t.get(i)).getValue();
		}
		nPartitions = nPartitionsPerNode * context.getNumberNodes();

		// Init variables
		bucketsCache = new Bucket[nPartitions];
		partitioner = null;
	}

	@Override
	public void process(Tuple inputTuple, ActionContext context,
			ActionOutput output) {
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
				b = context.startTransfer(nodeNo, bucketNo, shouldSort,
						sortingFields);
				bucketsCache[partition] = b;
			}
			b.add(inputTuple);
		} catch (Exception e) {
			log.error("Failed processing tuple.", e);
		}
	}

	@Override
	public void stopProcess(ActionContext context, ActionOutput output) {
		try {

			for (int i = 0; i < nPartitions; ++i) {
				int nodeNo = i / nPartitionsPerNode;
				int bucketNo = bucketIds[i % nPartitionsPerNode];
				context.finishTransfer(nodeNo, bucketNo, shouldSort,
						sortingFields, bucketsCache[i] != null);
			}

			// Send the chains to process the buckets to all the nodes that
			// will host the buckets
			if (context.isPrincipalBranch()) {
				for (int i = 1; i < nPartitionsPerNode; i++) {
					ActionConf c = ActionFactory
							.getActionConf(ReadFromBuckets.class);
					c.setParam(ReadFromBuckets.BUCKET_ID, bucketIds[i]);
					c.setParam(ReadFromBuckets.NODE_ID, -1);
					output.branch(c);
				}
			}
		} catch (Exception e) {
			log.error("Error", e);
		}
		bucketsCache = null;
		partitioner = null;
	}
}
