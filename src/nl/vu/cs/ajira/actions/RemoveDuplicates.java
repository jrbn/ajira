package nl.vu.cs.ajira.actions;

import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.data.types.TupleFactory;

/**
 * This action removes duplicates from sorted input.
 */
public class RemoveDuplicates extends Action {

	private final Tuple tuple = TupleFactory.newTuple();
	private boolean first = true;

	@Override
	public void startProcess(ActionContext context) {
		first = true;
	}

	@Override
	/**
	 * Only outputs the input tuple when it is not equal to the one before.
	 * @param tuple
	 *            the input tuple.
	 * @param context
	 *            the context in which this action is executed.
	 * @param actionOutput
	 *            to pass on the result of processing the input tuple.
	 * @throws Exception
	 */
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
