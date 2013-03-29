package nl.vu.cs.ajira.actions;

import nl.vu.cs.ajira.data.types.Tuple;

public class NotifyAll extends Action {

	public static final int I_TOKEN = 0;
	int token;

	@Override
	public void registerActionParameters(ActionConf conf) {
		conf.registerParameter(I_TOKEN, "I_TOKEN", null, true);
	}

	@Override
	public void startProcess(ActionContext context) throws Exception {
		token = getParamInt(I_TOKEN);
	}

	@Override
	public void process(Tuple tuple, ActionContext context,
			ActionOutput actionOutput) throws Exception {
		actionOutput.output(tuple);
	}

	@Override
	public void stopProcess(ActionContext context, ActionOutput actionOutput)
			throws Exception {
		context.signal(token);
	}

}
