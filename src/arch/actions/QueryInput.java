package arch.actions;

import arch.data.types.Tuple;
import arch.datalayer.Query;
import arch.utils.Consts;

public class QueryInput extends Action {

	public static final int TUPLE = 0;
	public static final String S_TUPLE = "tuple";
	public static final int INPUT_LAYER = 1;
	public static final String S_INPUT_LAYER = "input_layer";

	static class ParametersProcessor extends ActionConf.Configurator {
		@Override
		void setupConfiguration(Query query, Object[] params,
				ActionController controller, ActionContext context) {
			query.setInputLayer(((Integer) params[INPUT_LAYER]).intValue());
			query.setInputTuple((Tuple) params[TUPLE]);
			controller.doNotAddAction();
		}
	}

	@Override
	public void registerActionParameters(ActionConf conf) throws Exception {
		conf.registerParameter(TUPLE, S_TUPLE, null, true);
		conf.registerParameter(INPUT_LAYER, S_INPUT_LAYER,
				Consts.DEFAULT_INPUT_LAYER_ID, false);
		conf.registerCustomConfigurator(ParametersProcessor.class);
	}

	@Override
	public void process(Tuple tuple, ActionContext context, ActionOutput output)
			throws Exception {
		output.output(tuple);
	}

}
