package nl.vu.cs.ajira.examples.aurora.actions.io.test;

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

public class RandomTupleReaderAction extends Action {
	public static final int I_ID = 0;
	public static final int I_THREAD_ID = 1;

	public static void addToChain(int id, int threadId, ActionSequence actions)
			throws ActionNotConfiguredException {
		ActionConf c = ActionFactory
				.getActionConf(RandomTupleReaderAction.class);
		c.setParamInt(I_ID, id);
		c.setParamInt(I_THREAD_ID, threadId);
		actions.add(c);
	}

	@Override
	public void registerActionParameters(ActionConf conf) {
		conf.registerCustomConfigurator(new ParametersProcessor());
		conf.registerParameter(I_ID, "id", 0, true);
		conf.registerParameter(I_THREAD_ID, "thread id", 0, true);
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
			query.setInputLayer(RandomGeneratorInputLayer.class);
			Query q = new Query(new TInt((Integer) params[I_ID]), new TInt(
					(Integer) params[I_THREAD_ID]));
			query.setQuery(q);
			controller.doNotAddCurrentAction();
		}
	}

}
