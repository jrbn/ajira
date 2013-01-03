package arch.actions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import arch.buckets.Bucket;
import arch.data.types.Tuple;
import arch.datalayer.Query;

public class CollectTuples extends Action {

	static final Logger log = LoggerFactory.getLogger(CollectTuples.class);

	/* PARAMETERS */
	public static final int NODE_ID = 0;
	public static final String S_NODE_ID = "node_id";
	public static final int BUCKET_ID = 1;
	public static final String S_BUCKET_ID = "bucket_id";
	public static final int SORTING_FUNCTION = 2;
	public static final String S_SORTING_FUNCTION = "sorting_function";

	private int nodeId;
	private int bucketId = -1;
	private String sortingFunction = null;
	private Bucket bucket;

	static class ParametersProcessor extends
			ActionConf.RuntimeParameterProcessor {
		@Override
		public void processParameters(Query query, Object[] params,
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
		conf.registerParameter(NODE_ID, S_NODE_ID, null, false);
		conf.registerParameter(BUCKET_ID, S_BUCKET_ID, null, false);
		conf.registerParameter(SORTING_FUNCTION, S_SORTING_FUNCTION, null,
				false);
		conf.registerRuntimeParameterProcessor(ParametersProcessor.class);
	}

	@Override
	public void startProcess(ActionContext context) throws Exception {
		nodeId = getParamInt(NODE_ID);
		sortingFunction = getParamString(SORTING_FUNCTION);
		bucket = null;
	}

	@Override
	public void process(Tuple inputTuple, ActionContext context,
			ActionOutput output) {
		try {
			if (bucket == null) {
				bucket = context.startTransfer(nodeId, bucketId,
						sortingFunction);
			}
			bucket.add(inputTuple);
		} catch (Exception e) {
			log.error("Failed processing tuple.");
		}
	}

	@Override
	public void stopProcess(ActionContext context, ActionOutput output) {
		try {
			// Send the chains to process the buckets to all the nodes that
			// will host the buckets
			if (output.isBranchingAllowed()) {
				ActionConf c = ActionFactory
						.getActionConf(ReadFromBucket.class);
				c.setParam(ReadFromBucket.BUCKET_ID, bucketId);
				c.setParam(ReadFromBucket.NODE_ID, nodeId);
				output.branch(c);
			}

			context.finishTransfer(nodeId, bucketId, sortingFunction,
					bucket != null);

		} catch (Exception e) {
			log.error("Error", e);
		}
		bucket = null;
	}
}
