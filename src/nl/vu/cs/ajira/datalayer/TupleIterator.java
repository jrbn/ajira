package nl.vu.cs.ajira.datalayer;

import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.chains.ChainNotifier;
import nl.vu.cs.ajira.data.types.Tuple;

public abstract class TupleIterator {

	long counter;
	ActionContext c;
	String textCounter;

	/**
	 * It initialize the number of Tuples read,
	 * the ActionContext and the name of the counter
	 * that is used to keep track of the number of
	 * Tuples read. 
	 * 
	 * @param c
	 * 		The current ActionContext.
	 * @param inputLayer
	 * 		The name of the InputLayer.
	 */
	public void init(ActionContext c, String inputLayer) {
		counter = 0;
		this.c = c;
		textCounter = "Records Read From Input: " + inputLayer;
	}

	/**
	 * 
	 * 
	 * @return
	 * 		True if there is a next Tuple 
	 * 		and false otherwise.
	 * @throws Exception
	 */
	public boolean nextTuple() throws Exception {
		if (next()) {
			counter++;
			if (counter % 1000 == 0) {
				c.incrCounter(textCounter, counter);
				counter = 0;
			}
			return true;
		} else {
			c.incrCounter(textCounter, counter);
			counter = 0;
			return false;
		}
	}

	protected abstract boolean next() throws Exception;

	public abstract void getTuple(Tuple tuple) throws Exception;

	public abstract boolean isReady();

	public void registerReadyNotifier(ChainNotifier notifier) {
		// Default implementation is empty. Iterators that can be "not ready"
		// should implement this method, and call the notifiers "markReady()"
		// method
		// when the iterator becomes ready.
	}
}
