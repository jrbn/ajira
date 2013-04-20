package nl.vu.cs.ajira.actions;

import nl.vu.cs.ajira.actions.support.Query;
import nl.vu.cs.ajira.data.types.TStringArray;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.data.types.bytearray.BDataInput;
import nl.vu.cs.ajira.datalayer.InputLayer;
import nl.vu.cs.ajira.datalayer.InputQuery;

public class QueryInputLayer extends Action {

	public static final int W_QUERY = 0;
	public static final int S_INPUTLAYER = 1;
	public static final int SA_SIGNATURE_QUERY = 2;
	public static final String DEFAULT_LAYER = "DEFAULT";

	private static class ParametersProcessor extends ActionConf.Configurator {
		@Override
		public void setupAction(InputQuery query, Object[] params,
				ActionController controller, ActionContext context)
				throws Exception {
			String inputLayer = (String) params[S_INPUTLAYER];
			if (!inputLayer.equals(DEFAULT_LAYER)) {
				query.setInputLayer(Class.forName(inputLayer).asSubclass(
						InputLayer.class));
			} else {
				query.setInputLayer(InputLayer.DEFAULT_LAYER);
			}
			Query q = null;
			if (params[W_QUERY] instanceof byte[]) {
				// TODO: this is useless, I think! The readFrom method
				// completely
				// ignores any signature that the fields may have beforehand.
				// --Ceriel
				// Only allow Query here? Then the signature parameter can be
				// removed. --Ceriel
				if (params[SA_SIGNATURE_QUERY] != null) {
					TStringArray p = (TStringArray) params[SA_SIGNATURE_QUERY];
					q = new Query(p.getArray());
				} else {
					q = new Query();
				}

				q.readFrom(new BDataInput((byte[]) params[W_QUERY]));
			} else {
				q = (Query) params[W_QUERY];
			}
			query.setQuery(q);
			controller.doNotAddCurrentAction();
		}
	}

	@Override
	public void registerActionParameters(ActionConf conf) {
		conf.registerParameter(W_QUERY, "QUERY", null, true);
		conf.registerParameter(S_INPUTLAYER, "InputLayer",
				InputLayer.DEFAULT_LAYER.getName(), false);
		conf.registerParameter(SA_SIGNATURE_QUERY, "SA_SIGNATURE_QUERY", null,
				false);
		conf.registerCustomConfigurator(new ParametersProcessor());
	}

	@Override
	public void process(Tuple tuple, ActionContext context, ActionOutput output)
			throws Exception {
		output.output(tuple);
	}

}
