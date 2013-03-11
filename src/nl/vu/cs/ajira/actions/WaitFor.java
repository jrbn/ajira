package nl.vu.cs.ajira.actions;

import nl.vu.cs.ajira.data.types.Tuple;

public class WaitFor extends Action {

	public static final int TOKEN = 0;

	@Override
	public void registerActionParameters(ActionConf conf) {
		conf.registerParameter(TOKEN, "token", null, true);
	}

	@Override
	public void startProcess(ActionContext context) throws Exception {
		int token = getParamInt(TOKEN);
		context.waitFor(token);
	}

	@Override
	public void process(Tuple tuple, ActionContext context,
			ActionOutput actionOutput) throws Exception {
		actionOutput.output(tuple);
	}
}
