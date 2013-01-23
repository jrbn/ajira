package nl.vu.cs.ajira.actions;

import nl.vu.cs.ajira.data.types.Tuple;

public class RemoveDuplicates extends Action {

	private final Tuple tuple = new Tuple();
	boolean first = true;

	@Override
	public void startProcess(ActionContext context) throws Exception {
		first = true;
	}

	@Override
	public void process(Tuple inputTuple, ActionContext context, ActionOutput output)
			throws Exception {
		if (first) {
			inputTuple.copyTo(tuple);
			output.output(inputTuple);
			first = false;
		} else if (inputTuple.compareTo(tuple) != 0) {
			inputTuple.copyTo(tuple);
			output.output(inputTuple);
		}
	}
}
