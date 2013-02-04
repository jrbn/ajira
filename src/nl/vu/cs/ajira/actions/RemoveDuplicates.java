package nl.vu.cs.ajira.actions;

import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.data.types.TupleFactory;

public class RemoveDuplicates extends Action {

	private final Tuple tuple = TupleFactory.newTuple();
	boolean first = true;

	@Override
	public void startProcess(ActionContext context) throws Exception {
		first = true;
	}

	@Override
	public void process(Tuple inputTuple, ActionContext context,
			ActionOutput output) throws Exception {
		if (first) {
			inputTuple.copyTo(tuple);
			output.output(inputTuple);
			first = false;
		} else if (!inputTuple.equals(tuple)) {
			inputTuple.copyTo(tuple);
			output.output(inputTuple);
		}
	}
}
