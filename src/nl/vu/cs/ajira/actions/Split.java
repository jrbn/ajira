package nl.vu.cs.ajira.actions;

import nl.vu.cs.ajira.actions.support.WritableListActions;
import nl.vu.cs.ajira.data.types.Tuple;

public class Split extends Action {

	public static final int SPLIT = 0;
	private WritableListActions actions = new WritableListActions();
	private ActionOutput alternativePath = null;

	@Override
	public void registerActionParameters(ActionConf conf) {
		conf.registerParameter(SPLIT, "split", null, true);
	}

	@Override
	public void startProcess(ActionContext context) throws Exception {
		getParamWritable(actions, SPLIT);
		alternativePath = null;
	}

	@Override
	public void process(Tuple inputTuple, ActionContext context,
			ActionOutput output) throws Exception {
		if (alternativePath == null) {
			alternativePath = output.split(actions.getActions());
		}
		alternativePath.output(inputTuple);
		output.output(inputTuple);
	}
}
