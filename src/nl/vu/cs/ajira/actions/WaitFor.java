package nl.vu.cs.ajira.actions;

import nl.vu.cs.ajira.data.types.Tuple;

/**
 * This action waits for a specific token to be signaled in the {@link ActionContext}
 * before allowing the tuples to pass through unchanged.
 */
public class WaitFor extends Action {

	/**
	 * The <code>I_TOKEN</code> parameter, of type <code>int</code>, is required, and specifies
	 * the signal to wait for.
	 */
	public static final int I_TOKEN = 0;

	@Override
	public void registerActionParameters(ActionConf conf) {
		conf.registerParameter(I_TOKEN, "I_TOKEN", null, true);
	}

	@Override
	public void startProcess(ActionContext context) throws Exception {
		int token = getParamInt(I_TOKEN);
		context.waitFor(token);
	}

	@Override
	public void process(Tuple tuple, ActionContext context,
			ActionOutput actionOutput) throws Exception {
		actionOutput.output(tuple);
	}
}
