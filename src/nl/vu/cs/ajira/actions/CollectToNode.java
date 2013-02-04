package nl.vu.cs.ajira.actions;

import nl.vu.cs.ajira.buckets.Bucket;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.datalayer.Query;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CollectToNode extends Action {

	static final Logger log = LoggerFactory.getLogger(CollectToNode.class);

	/* PARAMETERS */
	public static final int NODE_ID = 0;
	private static final String S_NODE_ID = "node_id";
	private static final int BUCKET_ID = 1;
	private static final String S_BUCKET_ID = "bucket_id";
	public static final int SORT = 2;
	private static final String S_SORT = "sort";
	public static final int SORTING_FIELDS = 3;
	private static final String S_SORTING_FIELDS = "sorting_fields";
	public static final int TUPLE_FIELDS = 4;
	private static final String S_TUPLE_FIELDS = "tuple_fields";

	private int nodeId;
	private int bucketId = -1;
	private boolean sort;
	private byte[] sortingFields;
	private byte[] fields;
	private Bucket bucket;

	static class ParametersProcessor extends ActionConf.Configurator {
		@Override
		public void setupConfiguration(Query query, Object[] params,
				ActionController controller, ActionContext context) {
			if (params[NODE_ID] == null) {
				params[NODE_ID] = context.getMyNodeId();
			}
			params[BUCKET_ID] = context.getNewBucketID();
			controller.continueComputationOn(
					((Integer) params[NODE_ID]).intValue(),
					(Integer) params[BUCKET_ID]);
		}
	}

	@Override
	public void registerActionParameters(ActionConf conf) throws Exception {
		conf.registerParameter(NODE_ID, S_NODE_ID, null, false);
		conf.registerParameter(BUCKET_ID, S_BUCKET_ID, null, false);
		conf.registerParameter(SORT, S_SORT, false, false);
		conf.registerParameter(SORTING_FIELDS, S_SORTING_FIELDS, null, false);
		conf.registerParameter(TUPLE_FIELDS, S_TUPLE_FIELDS, null, true);
		conf.registerCustomConfigurator(ParametersProcessor.class);
	}

	@Override
	public void startProcess(ActionContext context) throws Exception {
		nodeId = getParamInt(NODE_ID);
		bucketId = getParamInt(BUCKET_ID);
		sort = getParamBoolean(SORT);
		sortingFields = getParamByteArray(SORTING_FIELDS);
		fields = getParamByteArray(TUPLE_FIELDS);
		bucket = null;
	}

	@Override
	public void process(Tuple inputTuple, ActionContext context,
			ActionOutput output) {
		try {
			if (bucket == null) {
				bucket = context.startTransfer(nodeId, bucketId, sort,
						sortingFields, fields);
			}
			bucket.add(inputTuple);
		} catch (Exception e) {
			log.error("Failed processing tuple.");
		}
	}

	@Override
	public void stopProcess(ActionContext context, ActionOutput output) {
		try {
			context.finishTransfer(nodeId, bucketId, sort, sortingFields,
					bucket != null, fields);

			// // Send the chains to process the buckets to all the nodes that
			// // will host the buckets
			// if (output.isRootBranch()) {
			// ActionConf c = ActionFactory
			// .getActionConf(ReadFromBucket.class);
			// c.setParam(ReadFromBucket.BUCKET_ID, bucketId);
			// c.setParam(ReadFromBucket.NODE_ID, nodeId);
			// output.branch(c);
			// }
		} catch (Exception e) {
			log.error("Error", e);
		}
		bucket = null;
	}
}
