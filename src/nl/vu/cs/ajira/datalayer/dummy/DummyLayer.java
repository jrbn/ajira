package nl.vu.cs.ajira.datalayer.dummy;

import java.io.IOException;

import nl.vu.cs.ajira.Context;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.chains.Location;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.data.types.TupleFactory;
import nl.vu.cs.ajira.datalayer.InputLayer;
import nl.vu.cs.ajira.datalayer.TupleIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DummyLayer extends InputLayer {

	public class DummyIterator extends TupleIterator {

		Tuple tuple = TupleFactory.newTuple();
		boolean first = true;

		/**
		 * Custom constructor.
		 * 
		 * @param c
		 * 		The current ActionContext.
		 * @param tuple
		 *		The new Tuple of the DummyIterator's tuple.  		
		 */
		public DummyIterator(ActionContext c, Tuple tuple) {
			super.init(c, "DummyInput");
			tuple.copyTo(this.tuple);
		}

		/**
		 * If it is the first time this method is called it 
		 * returns true, otherwise it returns false.
		 */
		@Override
		public boolean next() throws Exception {
			if (first) {
				first = false;
				return true;
			}
			return false;
		}

		/**
		 * Returns the Tuple of the class.
		 */
		@Override
		public void getTuple(Tuple tuple) throws Exception {
			this.tuple.copyTo(tuple);
		}

		@Override
		public boolean isReady() {
			return true;
		}
	}

	static final Logger log = LoggerFactory.getLogger(DummyLayer.class);
	int myId;

	/**
	 * Updates the id of the node.
	 */
	@Override
	protected void load(Context context) throws IOException {
		// Nothing to do here
		myId = context.getNetworkLayer().getMyPartition();
	}

	/**
	 * Returns a new Iterator for the provided 
	 * tuple and context.
	 */
	@Override
	public TupleIterator getIterator(Tuple tuple, ActionContext context) {
		return new DummyIterator(context, tuple);
	}

	@Override
	public Location getLocations(Tuple tuple, ActionContext context) {
		return Location.THIS_NODE;
	}

	@Override
	public void releaseIterator(TupleIterator itr, ActionContext context) {
	}
}