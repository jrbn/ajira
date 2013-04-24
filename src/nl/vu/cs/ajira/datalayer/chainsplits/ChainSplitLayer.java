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
	
	private static class CircularBuffer {
		private Tuple data[];
		private int head;
		private int tail;
		private boolean closed;

		public CircularBuffer(int size) {
			data = new Tuple[size];
			head = tail = 0;
		}

		public synchronized void push(Tuple v) {
			while (bufferFull()) {
				try {
					wait();
				} catch(InterruptedException e) {
					// ignore
				}
			}
			if (head == tail) {
				notify();
			}
			if (data[tail] == null) {
				data[tail] = TupleFactory.newTuple();
			}
			v.copyTo(data[tail++]);
			if (tail == data.length) {
				tail = 0;
			}
		}
		
		public synchronized void push(SimpleData... v) {
			while (bufferFull()) {
				try {
					wait();
				} catch(InterruptedException e) {
					// ignore
				}
			}
			if (head == tail) {
				notify();
			}
			if (data[tail] == null) {
				data[tail] = TupleFactory.newTuple();
			}
			data[tail++].set(v);
			if (tail == data.length) {
				tail = 0;
			}
		}


		public synchronized Tuple pop() {
			while (head == tail && ! closed) {
				try {
					wait();
				} catch(InterruptedException e) {
					// ignore
				}
			}
			if (head == tail) {
				data = null;
				return null;
			}
			if (bufferFull()) {
				notify();
			}
			Tuple v = data[head++];
			if (head == data.length) {
				head = 0;
			}
			return v;
		}
		
		public synchronized void close() {
			closed = true;
			notify();
		}

		private boolean bufferFull() {
			if (tail + 1 == head) {
				return true;
			}
			if (tail == (data.length - 1) && head == 0) {
				return true;
			}
			return false;
		}
	}

	public static class SplitIterator extends TupleIterator implements
			ActionOutput {

		private final int id;
		private final CircularBuffer tuples = new CircularBuffer(50000);
		private Tuple tuple;

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
		public boolean next() {
			tuple = tuples.pop();
			return tuple != null;
		}

		@Override
		public void getTuple(Tuple tuple) {
			this.tuple.copyTo(tuple);

		}

		@Override
		public boolean isReady() {
			return true;
		}

		@Override
		public void output(Tuple tuple) {
			tuples.push(tuple);
		}

		public void close() {
			tuples.close();
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

		@Override
		public synchronized void output(SimpleData... data) {
			tuples.push(data);
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
