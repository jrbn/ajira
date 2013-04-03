package nl.vu.cs.ajira.datalayer.chainsplits;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import nl.vu.cs.ajira.Context;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.actions.ActionSequence;
import nl.vu.cs.ajira.chains.ChainLocation;
import nl.vu.cs.ajira.data.types.SimpleData;
import nl.vu.cs.ajira.data.types.TInt;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.data.types.TupleFactory;
import nl.vu.cs.ajira.datalayer.InputLayer;
import nl.vu.cs.ajira.datalayer.TupleIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ChainSplitLayer extends InputLayer {

	static final Logger log = LoggerFactory.getLogger(ChainSplitLayer.class);

	public static class SplitIterator extends TupleIterator implements
			ActionOutput {

		private final int id;
		private boolean isOpen = true;
		private Tuple tuple = null;

		/**
		 * Custom constructor.
		 * 
		 * @param id
		 *            The id of the SplitIterator.
		 */
		SplitIterator(int id) {
			this.id = id;
		}

		/**
		 * 
		 * @return The id of the SplitIterator
		 */
		public int getId() {
			return id;
		}

		@Override
		public synchronized boolean next() {
			while (isOpen && tuple == null) {
				try {
					wait();
				} catch (InterruptedException e) {
					// ignore
				}
			}
			return isOpen;
		}

		@Override
		public synchronized void getTuple(Tuple tuple) {
			this.tuple.copyTo(tuple);
			this.tuple = null;
			notify();
		}

		@Override
		public boolean isReady() {
			return true;
		}

		@Override
		public synchronized void output(Tuple tuple) {
			this.tuple = tuple;
			notify();
			try {
				wait();
			} catch (InterruptedException e) {
				// ignore
			}
		}

		public synchronized void close() {
			isOpen = false;
			this.notify();
		}

		@Override
		public void branch(ActionSequence actions) throws Exception {
			throw new Exception("Not allowed");
		}

		@Override
		public ActionOutput split(int reconnectAt, ActionSequence actions)
				throws Exception {
			throw new Exception("Not allowed");
		}

		// FIXME: Very very inefficient. Need to fix it!
		@Override
		public synchronized void output(SimpleData... data) {
			this.tuple = TupleFactory.newTuple(data);
			notify();
			try {
				wait();
			} catch (InterruptedException e) {
				// ignore
			}
		}
	}

	private static final ChainSplitLayer instance = new ChainSplitLayer();
	public Map<Integer, SplitIterator> existingSplits = new ConcurrentHashMap<Integer, SplitIterator>();
	private int counter = 0;

	private ChainSplitLayer() {
	}

	public static ChainSplitLayer getInstance() {
		return instance;
	}

	public synchronized SplitIterator registerNewSplit() {
		SplitIterator itr = new SplitIterator(counter);
		existingSplits.put(counter++, itr);
		return itr;
	}

	@Override
	protected void load(Context context) throws Exception {
		// Nothing to do here
	}

	@Override
	public TupleIterator getIterator(Tuple tuple, ActionContext context) {
		try {
			SplitIterator itr = existingSplits.get(((TInt) tuple.get(0))
					.getValue());
			itr.init(context, "ChainSplitsLayer");
			return itr;
		} catch (Exception e) {
			log.error("Error in processing the input tuple", e);
		}
		return null;
	}

	@Override
	public void releaseIterator(TupleIterator itr, ActionContext context) {
		existingSplits.remove(((SplitIterator) itr).getId());
	}

	@Override
	public ChainLocation getLocations(Tuple tuple, ActionContext context) {
		return new ChainLocation(context.getMyNodeId());
	}
}
