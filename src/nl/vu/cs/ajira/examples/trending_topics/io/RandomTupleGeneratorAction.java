package nl.vu.cs.ajira.examples.trending_topics.io;

import nl.vu.cs.ajira.actions.Action;
import nl.vu.cs.ajira.actions.ActionConf;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionController;
import nl.vu.cs.ajira.actions.ActionFactory;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.actions.ActionSequence;
import nl.vu.cs.ajira.actions.support.Query;
import nl.vu.cs.ajira.data.types.SimpleData;
import nl.vu.cs.ajira.data.types.TInt;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.datalayer.InputQuery;
import nl.vu.cs.ajira.exceptions.ActionNotConfiguredException;

public class RandomTupleGeneratorAction extends Action {
	public static final int I_NUM_TUPLES = 0;
	public static final int I_NUM_DISTINCT_WORDS = 1;
	public static final int I_RANDOM_SEED = 2;

	public static void addToChain(int numTuples, int numDistinctWords,
			int randomSeed, ActionSequence actions)
			throws ActionNotConfiguredException {
		ActionConf c = ActionFactory
				.getActionConf(RandomTupleGeneratorAction.class);
		c.setParamInt(I_NUM_TUPLES, numTuples);
		c.setParamInt(I_NUM_DISTINCT_WORDS, numDistinctWords);
		c.setParamInt(I_RANDOM_SEED, randomSeed);
		actions.add(c);
	}

	@Override
	public void registerActionParameters(ActionConf conf) {
		conf.registerCustomConfigurator(new ParametersProcessor());
		conf.registerParameter(I_NUM_TUPLES, "number of tuples", 1, true);
		conf.registerParameter(I_NUM_DISTINCT_WORDS,
				"number of possible words", 100, true);
		conf.registerParameter(I_RANDOM_SEED, "random seed", 0, true);
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
			SimpleData[] data = new SimpleData[3];
			data[0] = new TInt((Integer) params[I_NUM_TUPLES]);
			data[1] = new TInt((Integer) params[I_NUM_DISTINCT_WORDS]);
			data[2] = new TInt((Integer) params[I_RANDOM_SEED]);
			Query q = new Query(data);
			query.setQuery(q);
			controller.doNotAddCurrentAction();
		}
	}

}
