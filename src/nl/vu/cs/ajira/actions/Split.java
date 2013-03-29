package nl.vu.cs.ajira.actions;

import nl.vu.cs.ajira.actions.support.WritableListActions;
import nl.vu.cs.ajira.data.types.Tuple;

public class Split extends Action {

	public static final int SPLIT = 0;
	public static final int I_RECONNECT_AFTER_ACTIONS = 1;
	private WritableListActions actions = new WritableListActions();
	private ActionOutput alternativePath = null;
	private int reconnectAt;

	@Override
	public void registerActionParameters(ActionConf conf) {
		conf.registerParameter(SPLIT, "SPLIT", null, false);
		conf.registerParameter(I_RECONNECT_AFTER_ACTIONS, "I_RECONNECT_AFTER_ACTIONS", -1,
				false);
	}

	@Override
	public void startProcess(ActionContext context) throws Exception {
		getParamWritable(actions, SPLIT);
		reconnectAt = getParamInt(I_RECONNECT_AFTER_ACTIONS);
		alternativePath = null;
	}

	@Override
	public void process(Tuple inputTuple, ActionContext context,
			ActionOutput output) throws Exception {
		if (alternativePath == null) {
			alternativePath = output.split(reconnectAt, actions.getActions());
		}
		alternativePath.output(inputTuple);
		output.output(inputTuple);
	}
}
