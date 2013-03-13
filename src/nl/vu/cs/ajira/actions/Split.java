package nl.vu.cs.ajira.actions;

import nl.vu.cs.ajira.actions.support.WritableListActions;
import nl.vu.cs.ajira.data.types.Tuple;

public class Split extends Action {

	public static final int SPLIT = 0;
	public static final int RECONNECT_AFTER_ACTIONS = 1;
	private WritableListActions actions = new WritableListActions();
	private ActionOutput alternativePath = null;
	private int reconnectAt;

	@Override
	public void registerActionParameters(ActionConf conf) {
		conf.registerParameter(SPLIT, "split", null, true);
		conf.registerParameter(RECONNECT_AFTER_ACTIONS, "reconnect at", -1,
				false);
	}

	@Override
	public void startProcess(ActionContext context) throws Exception {
		getParamWritable(actions, SPLIT);
		reconnectAt = getParamInt(RECONNECT_AFTER_ACTIONS);
		alternativePath = null;
	}

	@Override
	public void process(Tuple inputTuple, ActionContext context,
			ActionOutput output) throws Exception {
		if (alternativePath == null) {
			alternativePath = output.split(actions.getActions(), reconnectAt);
		}
		alternativePath.output(inputTuple);
		output.output(inputTuple);
	}
}
