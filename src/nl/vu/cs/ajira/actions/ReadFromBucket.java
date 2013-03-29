package nl.vu.cs.ajira.actions;

import nl.vu.cs.ajira.actions.support.Query;
import nl.vu.cs.ajira.data.types.TInt;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.datalayer.InputQuery;
import nl.vu.cs.ajira.utils.Consts;

public class ReadFromBucket extends Action {

	int node;
	int bucketId;
	boolean branch = false;

	public static final int I_BUCKET_ID = 0;
	public static final int I_NODE_ID = 1;

	static class ParametersProcessor extends ActionConf.Configurator {
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
	public void startProcess(ActionContext context) throws Exception {
		bucketId = getParamInt(I_BUCKET_ID);
		node = getParamInt(I_NODE_ID);
	}

	@Override
	public void process(Tuple inputTuple, ActionContext context,
			ActionOutput output) throws Exception {
		output.output(inputTuple);
	}
}
