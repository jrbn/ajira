package nl.vu.cs.ajira.actions;

import nl.vu.cs.ajira.actions.support.Query;
import nl.vu.cs.ajira.data.types.TInt;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.datalayer.InputQuery;
import nl.vu.cs.ajira.utils.Consts;

/**
 * The <code>ReadFromBucket</code> action obtains input tuples from a specified
 * bucket and passes them on to the {@link ActionOutput}. This may cause the
 * chain to be executed on all nodes, depending on the specified node in the
 * configuration parameters.
 */
public class ReadFromBucket extends Action {

	/**
	 * The <code>I_BUCKET_ID</code> parameter, of type <code>int</code>, is
	 * required, and represents the number of the bucket to obtain the input
	 * tuples from.
	 */
	public static final int I_BUCKET_ID = 0;

	/**
	 * The <code>I_NODE_ID</code> parameter, of type <code>int</code>, is not
	 * required. It either specifies a node number, on which the chain is to be
	 * executed, or -1, in which case the chain is executed on all nodes. The
	 * default value is -1.
	 */
	public static final int I_NODE_ID = 1;

	private static class ParametersProcessor extends ActionConf.Configurator {
		@Override
		public void setupAction(InputQuery query, Object[] params,
				ActionController controller, ActionContext context) {
			if (params[I_NODE_ID] == null) {
				params[I_NODE_ID] = -1;
			}

			query.setInputLayer(Consts.BUCKET_INPUT_LAYER_ID);
			query.setQuery(new Query(new TInt((Integer) params[I_BUCKET_ID]),
					new TInt((Integer) params[I_NODE_ID])));

			controller.doNotAddCurrentAction();
		}
	}

	@Override
	public void registerActionParameters(ActionConf conf) {
		conf.registerParameter(I_BUCKET_ID, "BUCKET_ID", null, true);
		conf.registerParameter(I_NODE_ID, "NODE_ID", -1, false);
		conf.registerCustomConfigurator(new ParametersProcessor());
	}

	@Override
	public void process(Tuple inputTuple, ActionContext context,
			ActionOutput output) throws Exception {
		output.output(inputTuple);
	}
}
