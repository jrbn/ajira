package nl.vu.cs.ajira.actions;

import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.data.types.TupleFactory;

/**
 * The <code>RemoveDuplicates</code> action removes duplicates from sorted input: only
 * unique input tuples are passed on to the {@link ActionOutput}.
 */
public class RemoveDuplicates extends Action {

	private final Tuple tuple = TupleFactory.newTuple();
	private boolean first = true;
	private long removed;
	private long different;

	@Override
	public void startProcess(ActionContext context) {
		first = true;
		removed = 0;
		different = 0;
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
			different++;
		} else if (inputTuple.compareTo(tuple) != 0) {
			inputTuple.copyTo(tuple);
			output.output(inputTuple);
			different++;
		} else {
			removed++;
		}
	}
	
	@Override
	public void stopProcess(ActionContext context, ActionOutput actionOutput) {
		context.incrCounter("Removed duplicates", removed);
		context.incrCounter("Different tuples", different);
	}
}
