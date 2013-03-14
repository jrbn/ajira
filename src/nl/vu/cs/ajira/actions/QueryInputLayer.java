package nl.vu.cs.ajira.actions;

import nl.vu.cs.ajira.actions.support.Query;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.datalayer.InputQuery;
import nl.vu.cs.ajira.utils.Consts;

public class QueryInputLayer extends Action {

	public static final int W_QUERY = 0;
	public static final int I_INPUTLAYER = 1;

	static class ParametersProcessor extends ActionConf.Configurator {
		@Override
		public void setupAction(InputQuery query, Object[] params,
				ActionController controller, ActionContext context) {
			query.setInputLayer(((Integer) params[I_INPUTLAYER]).intValue());
			Query t = (Query) params[W_QUERY];
			query.setQuery(t);
			controller.doNotAddCurrentAction();
		}
	}

	@Override
	public void registerActionParameters(ActionConf conf) {
		conf.registerParameter(W_QUERY, "query", null, true);
		conf.registerParameter(I_INPUTLAYER, "input layer",
				Consts.DEFAULT_INPUT_LAYER_ID, false);
		conf.registerCustomConfigurator(ParametersProcessor.class);
	}

	@Override
	public void process(Tuple tuple, ActionContext context, ActionOutput output)
			throws Exception {
		output.output(tuple);
	}

}
