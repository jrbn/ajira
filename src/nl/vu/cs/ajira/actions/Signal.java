package nl.vu.cs.ajira.actions;

import java.util.HashSet;
import java.util.Set;

import nl.vu.cs.ajira.data.types.Tuple;

public class Signal extends Action {

	private static final String TOKENS_OBJ = "Signal.TOKENS_OBJ";

	public static final int TOKEN = 0;
	int token;

	@Override
	public void registerActionParameters(ActionConf conf) throws Exception {
		conf.registerParameter(TOKEN, "token", null, true);
	}

	@Override
	public void startProcess(ActionContext context) throws Exception {
		token = getParamInt(TOKEN);
	}

	@Override
	public void process(Tuple tuple, ActionContext context,
			ActionOutput actionOutput) throws Exception {
		actionOutput.output(tuple);
	}

	@Override
	public void stopProcess(ActionContext context, ActionOutput actionOutput)
			throws Exception {
		if (context.isPrincipalBranch()) {
			@SuppressWarnings("unchecked")
			Set<Integer> set = (Set<Integer>) context
					.getObjectFromCache(TOKENS_OBJ);
			if (set == null) {
				set = new HashSet<>();
				context.putObjectInCache(TOKENS_OBJ, set);
			}
			set.add(token);
			context.broadcastCacheObjects(TOKENS_OBJ);
		}
	}

}
