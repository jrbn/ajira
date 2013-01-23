package nl.vu.cs.ajira.datalayer.chainsplits;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import nl.vu.cs.ajira.Context;
import nl.vu.cs.ajira.actions.ActionConf;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.chains.ChainLocation;
import nl.vu.cs.ajira.data.types.TInt;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.datalayer.InputLayer;
import nl.vu.cs.ajira.datalayer.TupleIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ChainSplitLayer extends InputLayer {

	static final Logger log = LoggerFactory.getLogger(ChainSplitLayer.class);

	public static class SplitIterator extends TupleIterator implements
			ActionOutput {

		private int id;
		private boolean isOpen = true;
		private Tuple tuple = null;

		SplitIterator(int id) {
			this.id = id;
		}

		int getId() {
			return id;
		}

		@Override
		public synchronized boolean next() throws Exception {
			while (isOpen && tuple == null) {
				wait();
			}
			return isOpen;
		}

		@Override
		public synchronized void getTuple(Tuple tuple) throws Exception {
			this.tuple.copyTo(tuple);
			this.tuple = null;
			notify();
		}

		@Override
		public boolean isReady() {
			return true;
		}

		@Override
		public synchronized void output(Tuple tuple) throws Exception {
			this.tuple = tuple;
			notify();
			wait();
		}

		public synchronized void close() {
			isOpen = false;
			this.notify();
		}

		@Override
		public void branch(List<ActionConf> actions) throws Exception {
			throw new Exception("Not allowed");
		}

		@Override
		public void branch(ActionConf action) throws Exception {
			throw new Exception("Not allowed");
		}

		@Override
		public ActionOutput split(List<ActionConf> actions) throws Exception {
			throw new Exception("Not allowed");
		}

		@Override
		public ActionOutput split(ActionConf action) throws Exception {
			throw new Exception("Not allowed");
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
		return null;
	}

	@Override
	protected void load(Context context) throws Exception {
		// Nothing to do here
	}

	@Override
	public TupleIterator getIterator(Tuple tuple, ActionContext context) {
		try {
			TInt v = new TInt();
			tuple.get(v, 0);
			return existingSplits.get(0);
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

	@Override
	public String getName() {
		return "ChainSplitLayer";
	}
}
