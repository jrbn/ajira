package nl.vu.cs.ajira.examples.aurora.test;

import nl.vu.cs.ajira.actions.Action;
import nl.vu.cs.ajira.actions.ActionConf;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionController;
import nl.vu.cs.ajira.actions.ActionFactory;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.actions.ActionSequence;
import nl.vu.cs.ajira.actions.support.Query;
import nl.vu.cs.ajira.data.types.TInt;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.datalayer.InputQuery;
import nl.vu.cs.ajira.exceptions.ActionNotConfiguredException;

public class RandomInputAction extends Action {
	public static void addToChain(int num, ActionSequence actions)
			throws ActionNotConfiguredException {
		ActionConf c = ActionFactory.getActionConf(RandomInputAction.class);
		c.setParamInt(RandomInputAction.NUM, num);
		actions.add(c);
	}

	public static final int NUM = 0;

	@Override
	public void registerActionParameters(ActionConf conf) {
		conf.registerCustomConfigurator(new ParametersProcessor());
		conf.registerParameter(NUM, "NUM", 0, true);
	}

	@Override
	public void process(Tuple tuple, ActionContext context, ActionOutput output)
			throws Exception {
		output.output(tuple);
	}

	private static class ParametersProcessor extends ActionConf.Configurator {
		@Override
		public void setupAction(InputQuery query, Object[] params,
				ActionController controller, ActionContext context)
				throws Exception {
			query.setInputLayer(RandomInputLayer.class);
			int num = (Integer) params[NUM];
			Query q = new Query(new TInt(num));
			query.setQuery(q);
			controller.doNotAddCurrentAction();
		}
	}

}
