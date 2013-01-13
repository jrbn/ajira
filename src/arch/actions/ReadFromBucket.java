package arch.actions;

import arch.data.types.TInt;
import arch.data.types.Tuple;
import arch.datalayer.Query;
import arch.utils.Consts;

public class ReadFromBucket extends Action {

	int node;
	int bucketId;
	boolean branch = false;

	public static final int BUCKET_ID = 0;
	public static final String S_BUCKET_ID = "bucket_id";
	public static final int NODE_ID = 1;
	public static final String S_NODE_ID = "node_id";

	static class ParametersProcessor extends ActionConf.Configurator {
		@Override
		void setupConfiguration(Query query, Object[] params,
				ActionController controller, ActionContext context) {
			if (params[NODE_ID] == null) {
				params[NODE_ID] = -1;
			}

			query.setInputLayer(Consts.BUCKET_INPUT_LAYER_ID);
			query.setInputTuple(new Tuple(
					new TInt((Integer) params[BUCKET_ID]), new TInt(
							(Integer) params[NODE_ID])));

			controller.doNotAddAction();
		}
	}

	@Override
	public void registerActionParameters(ActionConf conf) throws Exception {
		conf.registerParameter(BUCKET_ID, S_BUCKET_ID, null, true);
		conf.registerParameter(NODE_ID, S_NODE_ID, -1, false);
		conf.registerCustomConfigurator(ParametersProcessor.class);
	}

	@Override
	public void startProcess(ActionContext context) throws Exception {
		bucketId = getParamInt(BUCKET_ID);
		node = getParamInt(NODE_ID);
	}

	@Override
	public void process(Tuple inputTuple, ActionContext context,
			ActionOutput output) throws Exception {
		output.output(inputTuple);
	}
}
