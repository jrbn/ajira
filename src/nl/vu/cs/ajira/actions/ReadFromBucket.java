package nl.vu.cs.ajira.actions;

import nl.vu.cs.ajira.actions.support.Query;
import nl.vu.cs.ajira.data.types.TInt;
import nl.vu.cs.ajira.data.types.TIntArray;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.datalayer.InputQuery;
import nl.vu.cs.ajira.datalayer.buckets.BucketsLayer;
import nl.vu.cs.ajira.datalayer.dummy.DummyLayer;

/**
 * The <code>ReadFromBucket</code> action obtains input tuples from a specified
 * bucket and passes them on to the {@link ActionOutput}. This may cause the
 * chain to be executed on all nodes, depending on the specified node in the
 * configuration parameters.
 */
public class ReadFromBucket extends Action {

	/**
	 * The <code>IA_BUCKET_IDS</code> parameter, of type <code>intarray</code>,
	 * is required, and represents the number of the buckets to obtain the input
	 * tuples from.
	 */
	public static final int IA_BUCKET_IDS = 0;

	/**
	 * The <code>IA_NODE_IDS</code> parameter, of type <code>intarray</code>, is
	 * not required. It either specifies a list of node numbers, on which the
	 * chain is to be executed, or -1, in which case the chain is executed on
	 * all nodes. The default value is -1.
	 */
	public static final int IA_NODE_IDS = 1;

	private static class ParametersProcessor extends ActionConf.Configurator {
		@Override
		public void setupAction(InputQuery query, Object[] params,
				ActionController controller, ActionContext context) {

			int[] nodeIds = null;
			if (params[IA_NODE_IDS] == null) {
				nodeIds = new int[1];
				nodeIds[0] = -1;
				params[IA_NODE_IDS] = nodeIds;
			} else {
				nodeIds = ((TIntArray) params[IA_NODE_IDS]).getArray();
			}
			int[] bucketIds = ((TIntArray) params[IA_BUCKET_IDS]).getArray();

			if (nodeIds.length == 1) {
				query.setInputLayer(BucketsLayer.class);
				query.setQuery(new Query(new TInt(bucketIds[0]), new TInt(
						nodeIds[0])));

				controller.doNotAddCurrentAction();
			} else {
				// In this case, I need to fire more chains with a branch
				query.setInputLayer(DummyLayer.class);
				query.setQuery(new Query(new TInt(0)));
			}
		}
	}

	@Override
	public void registerActionParameters(ActionConf conf) {
		conf.registerParameter(IA_BUCKET_IDS, "BUCKET_IDS", null, true);
		conf.registerParameter(IA_NODE_IDS, "NODE_IDS", -1, false);
		conf.registerCustomConfigurator(new ParametersProcessor());
	}

	@Override
	public void process(Tuple inputTuple, ActionContext context,
			ActionOutput output) throws Exception {
	}

	@Override
	public void stopProcess(ActionContext context, ActionOutput actionOutput)
			throws Exception {
		// If this action has been called, then it is because we need to fire
		// multiple queries.
		int[] nodeIds = getParamIntArray(IA_NODE_IDS);
		int[] bucketIds = getParamIntArray(IA_BUCKET_IDS);

		for (int i = 0; i < nodeIds.length; ++i) {
			ActionSequence as = new ActionSequence();

			ActionConf c = ActionFactory.getActionConf(ReadFromBucket.class);
			c.setParamIntArray(ReadFromBucket.IA_NODE_IDS, nodeIds[i]);
			c.setParamIntArray(ReadFromBucket.IA_BUCKET_IDS, bucketIds[i]);
			as.add(c);

			actionOutput.branch(as);
		}
	}
}
