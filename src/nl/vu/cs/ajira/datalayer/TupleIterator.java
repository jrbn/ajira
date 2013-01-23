package nl.vu.cs.ajira.datalayer;

import nl.vu.cs.ajira.chains.ChainNotifier;
import nl.vu.cs.ajira.data.types.Tuple;

public abstract class TupleIterator {

	public abstract boolean next() throws Exception;

	public abstract void getTuple(Tuple tuple) throws Exception;

	public abstract boolean isReady();
	
	public void registerReadyNotifier(ChainNotifier notifier) {
	    // Default implementation is empty. Iterators that can be "not ready"
	    // should implement this method, and call the notifiers "markReady()" method
	    // when the iterator becomes ready.
	}
}
