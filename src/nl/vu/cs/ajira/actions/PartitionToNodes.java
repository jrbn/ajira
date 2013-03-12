package nl.vu.cs.ajira.actions;

import nl.vu.cs.ajira.actions.support.HashPartitioner;
import nl.vu.cs.ajira.actions.support.Partitioner;
import nl.vu.cs.ajira.buckets.Bucket;
import nl.vu.cs.ajira.data.types.DataProvider;
import nl.vu.cs.ajira.data.types.TIntArray;
import nl.vu.cs.ajira.data.types.TStringArray;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.datalayer.Query;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PartitionToNodes extends Action {

	/* PARAMETERS */
	public static final int SORT = 0;
	private static final String S_SORT = "sort";
	public static final int PARTITIONER = 1;
	private static final String S_PARTITIONER = "partitioner";
	public static final int NPARTITIONS_PER_NODE = 2;
	private static final String S_NPARTITIONS_PER_NODE = "npartitions_per_node";
	private static final int BUCKET_IDS = 3;
	private static final String S_BUCKET_IDS = "bucket_ids";
	public static final int SORTING_FIELDS = 4;
	private static final String S_SORTING_FIELDS = "sorting_fields";
	public static final int TUPLE_FIELDS = 5;
	private static final String S_TUPLE_FIELDS = "tuple_fields";

	static final Logger log = LoggerFactory.getLogger(PartitionToNodes.class);

	private boolean shouldSort;
	private byte[] sortingFields = null;

	private byte[] tupleFields = null;

	private Bucket[] bucketsCache;
	private int nPartitionsPerNode;
	private String sPartitioner = null;
	private Partitioner partitioner = null;
	private int nPartitions;
	private int[] bucketIds;
	private boolean partition;

	public static class ParametersProcessor extends ActionConf.Configurator {
		@Override
		public void setupAction(Query query, Object[] params,
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
				TIntArray array = new TIntArray(np);
				for (int i = 0; i < np; ++i) {
					array.getArray()[i] = context.getNewBucketID();
				}
				params[BUCKET_IDS] = array;
			}

			// Convert the tuple fields in numbers
			TStringArray fields = (TStringArray) params[TUPLE_FIELDS];
			byte[] f = new byte[fields.getArray().length];
			int i = 0;
			for (String v : fields.getArray()) {
				f[i++] = (byte) DataProvider.getId(v);
			}
			params[TUPLE_FIELDS] = f;
			
			if (params[SORT] == null) {
				params[SORT] = new Boolean(false);
			}
			
			controller.continueComputationOn(-1,
					((TIntArray) params[BUCKET_IDS]).getArray()[0]);
		}
	}

	@Override
	public void registerActionParameters(ActionConf conf) {
		conf.registerParameter(SORT, S_SORT, false, false);
		conf.registerParameter(PARTITIONER, S_PARTITIONER,
				HashPartitioner.class.getName(), false);
		conf.registerParameter(NPARTITIONS_PER_NODE, S_NPARTITIONS_PER_NODE,
				null, false);
		conf.registerParameter(BUCKET_IDS, S_BUCKET_IDS, null, false);
		conf.registerParameter(SORTING_FIELDS, S_SORTING_FIELDS, null, false);
		conf.registerParameter(TUPLE_FIELDS, S_TUPLE_FIELDS, null, true);

		conf.registerCustomConfigurator(ParametersProcessor.class);
	}

	@Override
	public void startProcess(ActionContext context) throws Exception {
		shouldSort = getParamBoolean(SORT);
		sortingFields = getParamByteArray(SORTING_FIELDS);

		sPartitioner = getParamString(PARTITIONER);
		nPartitionsPerNode = getParamInt(NPARTITIONS_PER_NODE);

		bucketIds = getParamIntArray(BUCKET_IDS);
		nPartitions = nPartitionsPerNode * context.getNumberNodes();
		if (nPartitions > 1) {
			partition = true;
		} else {
			partition = false;
		}

		tupleFields = getParamByteArray(TUPLE_FIELDS);

		// Init variables
		bucketsCache = new Bucket[nPartitions];
		partitioner = null;
	}

	@Override
	public void process(Tuple inputTuple, ActionContext context,
			ActionOutput output) {
		try {

			Bucket b = null;
			if (partition) {
				// First partition the data
				if (partitioner == null) {
					partitioner = (Partitioner) Class.forName(sPartitioner)
							.newInstance();
					partitioner.init(context);
				}

				int partition = partitioner.partition(inputTuple, nPartitions);
				b = bucketsCache[partition];
				if (b == null) {
					int nodeNo = partition / nPartitionsPerNode;
					int bucketNo = bucketIds[partition % nPartitionsPerNode];
					b = context.startTransfer(nodeNo, bucketNo, shouldSort,
							sortingFields, tupleFields);
					bucketsCache[partition] = b;
				}
			} else {
				b = bucketsCache[0];
				if (b == null) {
					b = context.startTransfer(0, bucketIds[0], shouldSort,
							sortingFields, tupleFields);
					bucketsCache[0] = b;
				}
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
						sortingFields, bucketsCache[i] != null, tupleFields);
			}

			// Send the chains to process the buckets to all the nodes that
			// will host the buckets
			if (context.isPrincipalBranch()) {
				for (int i = 1; i < nPartitionsPerNode; i++) {
					ActionConf c = ActionFactory
							.getActionConf(ReadFromBucket.class);
					c.setParamInt(ReadFromBucket.BUCKET_ID, bucketIds[i]);
					c.setParamInt(ReadFromBucket.NODE_ID, -1);
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
