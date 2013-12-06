package nl.vu.cs.ajira.examples.aurora.actions.io.test;

import java.util.List;

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
import nl.vu.cs.ajira.data.types.TString;
import nl.vu.cs.ajira.data.types.TStringArray;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.datalayer.InputQuery;
import nl.vu.cs.ajira.exceptions.ActionNotConfiguredException;

public class RandomTupleGeneratorAction extends Action {
	public static final int I_ID = 0;
	public static final int I_NUM_THREADS = 1;
	public static final int SA_ATTRIBUTES = 2;
	public static final int I_NUM_TUPLES = 3;
	public static final int I_ATTR_MIN_VAL = 4;
	public static final int I_ATTR_MAX_VAL = 5;
	public static final int I_RANDOM_SEED = 6;

	private static final int numTuplesDefault = 1;
	private static final int attrMinValDefaut = 0;
	private static final int attrMaxValDefault = 100; // FIXME
														// Integer.MAX_VALUE;
	private static final int randomSeedDefault = 0;

	public static void addToChain(int id, int numThreads,
			List<String> attributes, int numTuples, int minVal, int maxVal,
			int randomSeed, ActionSequence actions)
			throws ActionNotConfiguredException {
		ActionConf c = ActionFactory
				.getActionConf(RandomTupleGeneratorAction.class);
		c.setParamInt(I_ID, id);
		c.setParamInt(I_NUM_THREADS, numThreads);
		c.setParamStringArray(SA_ATTRIBUTES,
				attributes.toArray(new String[attributes.size()]));
		c.setParamInt(I_NUM_TUPLES, numTuples);
		c.setParamInt(I_ATTR_MIN_VAL, minVal);
		c.setParamInt(I_ATTR_MAX_VAL, maxVal);
		c.setParamInt(I_RANDOM_SEED, randomSeed);
		actions.add(c);
	}

	public static void addToChain(List<String> attributes, int numThreads,
			int numTuples, int minVal, int maxVal, int randomSeed,
			ActionSequence actions) throws ActionNotConfiguredException {
		addToChain(0, numThreads, attributes, numTuples, minVal, maxVal,
				randomSeed, actions);
	}

	public static void addToChain(int id, int numThreads,
			List<String> attributes, int numTuples, int seed,
			ActionSequence actions) throws ActionNotConfiguredException {
		addToChain(id, numThreads, attributes, numTuples, attrMinValDefaut,
				attrMaxValDefault, seed, actions);
	}

	@Override
	public void registerActionParameters(ActionConf conf) {
		conf.registerCustomConfigurator(new ParametersProcessor());
		conf.registerParameter(I_ID, "id", 0, true);
		conf.registerParameter(I_NUM_THREADS, "number of threads", 0, true);
		conf.registerParameter(SA_ATTRIBUTES, "attributes", null, true);
		conf.registerParameter(I_NUM_TUPLES, "number of tuples",
				numTuplesDefault, false);
		conf.registerParameter(I_ATTR_MIN_VAL, "minimum value for parameters",
				attrMinValDefaut, false);
		conf.registerParameter(I_ATTR_MAX_VAL, "maximum value for parameters",
				attrMaxValDefault, false);
		conf.registerParameter(I_RANDOM_SEED, "random seed", randomSeedDefault,
				false);
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
			String[] attributes = ((TStringArray) params[SA_ATTRIBUTES])
					.getArray();
			SimpleData[] data = new SimpleData[6 + attributes.length];
			data[0] = new TInt((Integer) params[I_ID]);
			data[1] = new TInt((Integer) params[I_NUM_THREADS]);
			data[2] = new TInt((Integer) params[I_NUM_TUPLES]);
			data[3] = new TInt((Integer) params[I_ATTR_MIN_VAL]);
			data[4] = new TInt((Integer) params[I_ATTR_MAX_VAL]);
			data[5] = new TInt((Integer) params[I_RANDOM_SEED]);
			for (int i = 0; i < attributes.length; i++) {
				data[6 + i] = new TString(attributes[i]);
			}
			Query q = new Query(data);
			query.setQuery(q);
			controller.doNotAddCurrentAction();
		}
	}

}
