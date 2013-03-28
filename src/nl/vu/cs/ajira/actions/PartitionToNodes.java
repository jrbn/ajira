package nl.vu.cs.ajira.actions;

import nl.vu.cs.ajira.actions.support.HashPartitioner;
import nl.vu.cs.ajira.actions.support.Partitioner;
import nl.vu.cs.ajira.buckets.Bucket;
import nl.vu.cs.ajira.data.types.DataProvider;
import nl.vu.cs.ajira.data.types.TIntArray;
import nl.vu.cs.ajira.data.types.TStringArray;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.datalayer.InputQuery;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The <code>PartitionToNodes</code> action divides its input into partitions, one or more
 * per node participating in the run. Various parameters allow the user to control the
 * partitioning.
 */
public class PartitionToNodes extends Action {

	/* PARAMETERS */
	
	/**
	 *  The <code>SORT</code> parameter is of type <code>boolean</code>, is not required, and defaults
	 *  to <code>false</code>. When set, the resulting partitions will be sorted.
	 */
	public static final int SORT = 0;
	private static final String S_SORT = "SORT";
	
	/**
	 * The <code>PARTITIONER</code> parameter is of type <code>String</code>, is not required, and
	 * defaults to the class name of {@link HashPartitioner}. It indicates a class name of a
	 * class that must implement {@link Partitioner}, and must have a public parameterless constructor.
	 */
	public static final int PARTITIONER = 1;
	private static final String S_PARTITIONER = "PARTITIONER";
	
	/**
	 * The <code>NPARTITIONS_PER_NODE</code> parameter is of type <code>int</code>, is not required,
	 * and defaults to <code>1</code> if no destination buckets were specified, otherwise it defaults
	 * to the number of destination buckets. It indicates the number of partitions per node.
	 */
	public static final int NPARTITIONS_PER_NODE = 2;
	private static final String S_NPARTITIONS_PER_NODE = "NPARTITIONS_PER_NODE";
	
	/**
	 * The <code>BUCKET_IDS</code> parameter is of type {@link TIntArray} and is not required.
	 * If not specified, new bucket identifications are generated on the fly, as needed.
	 * When specified, it contains a list of integers, one bucket identifier for each partition
	 * per node.
	 */
	private static final int BUCKET_IDS = 3;
	private static final String S_BUCKET_IDS = "BUCKET_IDS";
	
	/**
	 * The <code>SORTING_FIELDS</code> parameter is of type <code>byte[]</code>, is not required,
	 * and is only significant when the result has to be sorted. In that case, this field determines
	 * which fields of the tuples are going to be considered when sorting. For instance, if the
	 * byte array contains <code>{2, 0}</code>, the sorting will use field 2 and field 0, in that
	 * order. When not specified, all fields are used, in ascending order.
	 */
	public static final int SORTING_FIELDS = 4;
	private static final String S_SORTING_FIELDS = "SORTING_FIELDS";
	
	/**
	 * The <code>TUPLE_FIELDS</code> parameter is of type <code>String[]</code>, is required, and
	 * specifies the class name of the type of each field in the tuple (see {@link nl.vu.cs.ajira.data.types}).
	 */
	public static final int TUPLE_FIELDS = 5;
	private static final String S_TUPLE_FIELDS = "TUPLE_FIELDS";
	
	/**
	 * The <code>PARTITION_FIELDS</code> parameter is of type <code>byte[]</code>, and is not required
	 * This field determines which fields of the tuples are going to be considered when partitioning
	 * (see {@link Partitioner#init(ActionContext, int, byte[])}.
	 */
	public static final int PARTITION_FIELDS = 6;
	private static final String S_PARTITION_FIELDS = "PARTITION_FIELDS";

	static final Logger log = LoggerFactory.getLogger(PartitionToNodes.class);

	private boolean shouldSort;
	private byte[] sortingFields = null;
	private byte[] partitionFields = null;

	private byte[] tupleFields = null;

	private Bucket[] bucketsCache;
	private int nPartitionsPerNode;
	private String sPartitioner = null;
	private Partitioner partitioner = null;
	private int nPartitions;
	private int[] bucketIds;
	private boolean partition;

	private static class ParametersProcessor extends ActionConf.Configurator {
		@Override
		public void setupAction(InputQuery query, Object[] params,
				ActionController controller, ActionContext context) {
			if (params[NPARTITIONS_PER_NODE] == null) {
				if (params[BUCKET_IDS] == null) {
					params[NPARTITIONS_PER_NODE] = 1;
				} else {
					TIntArray t = (TIntArray) params[BUCKET_IDS];
					params[NPARTITIONS_PER_NODE] = t.getArray().length;
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
		conf.registerParameter(PARTITION_FIELDS, S_PARTITION_FIELDS, null,
				false);

		conf.registerCustomConfigurator(new ParametersProcessor());
	}

	@Override
	public void startProcess(ActionContext context) throws Exception {
		shouldSort = getParamBoolean(SORT);
		sortingFields = getParamByteArray(SORTING_FIELDS);

		sPartitioner = getParamString(PARTITIONER);
		nPartitionsPerNode = getParamInt(NPARTITIONS_PER_NODE);
		partitionFields = getParamByteArray(PARTITION_FIELDS);

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
			ActionOutput output) throws Exception {
		Bucket b = null;
		if (partition) {
			// First partition the data
			if (partitioner == null) {
				partitioner = (Partitioner) Class.forName(sPartitioner)
						.newInstance();
				partitioner.init(context, nPartitions, partitionFields);
			}

			int partition = partitioner.partition(inputTuple);

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
	}

	@Override
	public void stopProcess(ActionContext context, ActionOutput output) throws Exception {
		
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

		bucketsCache = null;
		partitioner = null;
	}
}
