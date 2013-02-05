package nl.vu.cs.ajira.actions;

import nl.vu.cs.ajira.buckets.TupleSerializer;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.datalayer.Query;
import nl.vu.cs.ajira.utils.Consts;

public class SetQueryInputLayer extends Action {

	public static final int TUPLE = 0;
	public static final String S_TUPLE = "tuple";
	public static final int INPUT_LAYER = 1;
	public static final String S_INPUT_LAYER = "input_layer";

	static class ParametersProcessor extends ActionConf.Configurator {
		@Override
		void setupAction(Query query, Object[] params,
				ActionController controller, ActionContext context) {
			query.setInputLayer(((Integer) params[INPUT_LAYER]).intValue());
			TupleSerializer t = (TupleSerializer) params[TUPLE];
			query.setInputTuple(t.getTuple());
			controller.doNotAddCurrentAction();
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
