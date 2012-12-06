package arch.dummylayer;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import arch.ActionContext;
import arch.Context;
import arch.chains.Chain;
import arch.chains.ChainLocation;
import arch.data.types.Tuple;
import arch.datalayer.InputLayer;
import arch.datalayer.TupleIterator;

public class DummyLayer extends InputLayer {

	public class DummyIterator extends TupleIterator {

		Tuple tuple = new Tuple();
		boolean first = true;

		public DummyIterator(Tuple tuple) {
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
		return new DummyIterator(tuple);
	}

	@Override
	public ChainLocation getLocations(Tuple tuple, Chain chain, Context context) {
		return ChainLocation.THIS_NODE;
	}

	@Override
	public void releaseIterator(TupleIterator itr, ActionContext context) {
	}
}