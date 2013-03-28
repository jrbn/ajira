package nl.vu.cs.ajira.actions;

import nl.vu.cs.ajira.actions.support.Query;
import nl.vu.cs.ajira.data.types.TStringArray;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.data.types.bytearray.BDataInput;
import nl.vu.cs.ajira.datalayer.InputQuery;
import nl.vu.cs.ajira.utils.Consts;

public class QueryInputLayer extends Action {

	public static final int QUERY = 0;
	public static final int INPUTLAYER = 1;
	public static final int SIGNATURE_QUERY = 2;

	static class ParametersProcessor extends ActionConf.Configurator {
		@Override
		public void setupAction(InputQuery query, Object[] params,
				ActionController controller, ActionContext context)
				throws Exception {
			query.setInputLayer(((Integer) params[INPUTLAYER]).intValue());
			Query q = null;
			if (params[QUERY] instanceof byte[]) {
				if (params[SIGNATURE_QUERY] != null) {
					TStringArray p = (TStringArray) params[SIGNATURE_QUERY];
					q = new Query(p.getArray());
				} else {
					q = new Query();
				}

				q.readFrom(new BDataInput((byte[]) params[QUERY]));
			} else {
				q = (Query) params[QUERY];
			}
			query.setQuery(q);
			controller.doNotAddCurrentAction();
		}
	}

	@Override
	public void registerActionParameters(ActionConf conf) {
		conf.registerParameter(QUERY, "QUERY", null, true);
		conf.registerParameter(INPUTLAYER, "INPUTLAYER",
				Consts.DEFAULT_INPUT_LAYER_ID, false);
		conf.registerParameter(SIGNATURE_QUERY, "SIGNATURE_QUERY", null, false);
		conf.registerCustomConfigurator(new ParametersProcessor());
	}

	@Override
	public void process(Tuple tuple, ActionContext context, ActionOutput output)
			throws Exception {
		output.output(tuple);
	}

}
