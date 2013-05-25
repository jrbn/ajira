package nl.vu.cs.ajira.actions;

import nl.vu.cs.ajira.data.types.Tuple;

public class Branch extends Action {

	/***** PARAMETERS *****/
	public static final int W_BRANCH = 0;

	private boolean branched;

	@Override
	public void registerActionParameters(ActionConf conf) {
		conf.registerParameter(W_BRANCH, "W_BRANCH", null, true);
	}

	@Override
	public void startProcess(ActionContext context) throws Exception {
		branched = false;
	}

	@Override
	public void process(Tuple inputTuple, ActionContext context,
			ActionOutput output) throws Exception {
		if (!branched && context.isPrincipalBranch()) {
			ActionSequence branch = new ActionSequence();
			getParamWritable(branch, W_BRANCH);
			output.branch(branch);
			branched = true;
		}
		output.output(inputTuple);
	}

	@Override
	public void stopProcess(ActionContext context, ActionOutput output)
			throws Exception {
		if (!branched && context.isPrincipalBranch()) {
			ActionSequence branch = new ActionSequence();
			getParamWritable(branch, W_BRANCH);
			output.branch(branch);
		}
	}
}
