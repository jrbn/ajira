package nl.vu.cs.ajira.actions;

import nl.vu.cs.ajira.data.types.Tuple;

public class WaitFor extends Action {

	@Override
	public void startProcess(ActionContext context) throws Exception {
		// TODO: Wait until the token is inserted in the object
	}

	@Override
	public void process(Tuple tuple, ActionContext context,
			ActionOutput actionOutput) throws Exception {
		actionOutput.output(tuple);
	}
}
