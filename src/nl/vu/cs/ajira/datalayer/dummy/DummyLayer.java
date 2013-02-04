package nl.vu.cs.ajira.datalayer.dummy;

import java.io.IOException;

import nl.vu.cs.ajira.Context;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.chains.ChainLocation;
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

		public DummyIterator(ActionContext c, Tuple tuple) {
			super.init(c, "DummyInput");
			tuple.copyTo(this.tuple);
		}

		@Override
		public boolean next() throws Exception {
			if (first) {
				first = false;
				return true;
			}
			return false;
		}

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

	@Override
	protected void load(Context context) throws IOException {
		// Nothing to do here
		myId = context.getNetworkLayer().getMyPartition();
	}

	@Override
	public TupleIterator getIterator(Tuple tuple, ActionContext context) {
		return new DummyIterator(context, tuple);
	}

	@Override
	public ChainLocation getLocations(Tuple tuple, ActionContext context) {
		return ChainLocation.THIS_NODE;
	}

	@Override
	public void releaseIterator(TupleIterator itr, ActionContext context) {
	}
}