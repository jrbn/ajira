package nl.vu.cs.ajira.examples.trending_topics.io;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import nl.vu.cs.ajira.Context;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.chains.Location;
import nl.vu.cs.ajira.data.types.TInt;
import nl.vu.cs.ajira.data.types.TString;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.data.types.TupleFactory;
import nl.vu.cs.ajira.datalayer.InputLayer;
import nl.vu.cs.ajira.datalayer.TupleIterator;

public class RandomGeneratorInputLayer extends InputLayer {
	private static final int wordsLength = 25;
	private static final String chars = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";

	@Override
	protected void load(Context context) throws Exception {
		// Nothing to do
	}

	@Override
	public TupleIterator getIterator(Tuple tuple, ActionContext context) {
		return new RandomInputLayerIterator(tuple, context);
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

	class RandomInputLayerIterator extends TupleIterator {
		private final List<String> words = new ArrayList<String>();
		private final Random r;
		private final int numTuples;
		private int currentNumTuples = 0;

		RandomInputLayerIterator(Tuple tuple, ActionContext context) {
			init(context, "RandomInputLayerIterator");
			numTuples = ((TInt) tuple.get(0)).getValue();
			int numDistinctWords = ((TInt) tuple.get(1)).getValue();
			int seed = ((TInt) tuple.get(2)).getValue();
			r = new Random(seed);
			generateWords(numDistinctWords, r);
		}

		@Override
		protected final boolean next() throws Exception {
			return (numTuples < 0 || currentNumTuples < numTuples);
		}

		@Override
		public final void getTuple(Tuple tuple) throws Exception {
			generateTuple().copyTo(tuple);
			currentNumTuples++;
		}

		@Override
		public final boolean isReady() {
			return true;
		}

		private final Tuple generateTuple() {
			String wordString = words.get(r.nextInt(words.size()));
			TString word = new TString(wordString);
			return TupleFactory.newTuple(word);
		}

		private final void generateWords(int numDistinctWords, Random r) {
			for (int i = 0; i < numDistinctWords; i++) {
				words.add(generateWord(r));
			}
		}

		private final String generateWord(Random r) {
			StringBuilder sb = new StringBuilder(wordsLength);
			for (int i = 0; i < wordsLength; i++) {
				sb.append(chars.charAt(r.nextInt(chars.length())));
			}
			return sb.toString();
		}

	}

}
