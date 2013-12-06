package nl.vu.cs.ajira.examples.aurora.actions.io.test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

import nl.vu.cs.ajira.Context;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.chains.Location;
import nl.vu.cs.ajira.data.types.SimpleData;
import nl.vu.cs.ajira.data.types.TInt;
import nl.vu.cs.ajira.data.types.TString;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.data.types.TupleFactory;
import nl.vu.cs.ajira.datalayer.InputLayer;
import nl.vu.cs.ajira.datalayer.TupleIterator;

public class RandomGeneratorInputLayer extends InputLayer {
	private Map<Integer, Map<Integer, List<Tuple>>> tuples;

	@Override
	protected void load(Context context) throws Exception {
		tuples = new HashMap<Integer, Map<Integer, List<Tuple>>>();
	}

	@Override
	public TupleIterator getIterator(Tuple tuple, ActionContext context) {
		int id = ((TInt) tuple.get(0)).getValue();
		int threadId = ((TInt) tuple.get(1)).getValue();
		if (tuple.getNElements() > 2) {
			generateIteratorFor(tuple);
			return new RandomInputLayerIterator(new ArrayList<Tuple>());
		}
		return new RandomInputLayerIterator(tuples.get(id).get(threadId));
	}

	@Override
	public void releaseIterator(TupleIterator itr, ActionContext context) {
		// Nothing to do
	}

	@Override
	public Location getLocations(Tuple tuple, ActionContext context) {
		// For now it supports only a local machine.
		return Location.THIS_NODE;
	}

	private void generateIteratorFor(Tuple tuple) {
		int id = ((TInt) tuple.get(0)).getValue();
		int numThreads = ((TInt) tuple.get(1)).getValue();
		int numTuples = ((TInt) tuple.get(2)).getValue();
		int minValue = ((TInt) tuple.get(3)).getValue();
		int maxValue = ((TInt) tuple.get(4)).getValue();
		int seed = ((TInt) tuple.get(5)).getValue();
		List<String> attributes = new ArrayList<String>();
		for (int i = 6; i < tuple.getNElements(); i++) {
			String attr = ((TString) tuple.get(i)).getValue();
			attributes.add(attr);
		}
		Map<Integer, List<Tuple>> tupleMap = new HashMap<Integer, List<Tuple>>();
		Random r = new Random(seed);
		for (int t = 0; t < numThreads; t++) {
			List<Tuple> tupleList = generateTuples(numTuples / numThreads,
					minValue, maxValue, r, attributes);
			tupleMap.put(t, tupleList);
		}
		tuples.remove(id);
		tuples.put(id, tupleMap);
	}

	private List<Tuple> generateTuples(int numTuples, int minValue,
			int maxValue, Random r, List<String> attributes) {
		List<Tuple> result = new ArrayList<Tuple>();
		int tupleSize = attributes.size() * 2;
		for (int i = 0; i < numTuples; i++) {
			SimpleData[] data = new SimpleData[tupleSize];
			int pos = 0;
			for (String attr : attributes) {
				data[pos++] = new TString(attr);
				data[pos++] = new TInt(minValue
						+ r.nextInt(maxValue - minValue));
			}
			result.add(TupleFactory.newTuple(data));
		}
		return result;
	}

	class RandomInputLayerIterator extends TupleIterator {
		private final Iterator<Tuple> it;

		public RandomInputLayerIterator(List<Tuple> tuples) {
			it = tuples.iterator();
		}

		@Override
		protected boolean next() throws Exception {
			return it.hasNext();
		}

		@Override
		public void getTuple(Tuple tuple) throws Exception {
			it.next().copyTo(tuple);
		}

		@Override
		public boolean isReady() {
			return true;
		}

	}

}
