package nl.vu.cs.ajira.examples.trending_topics.actions;

import nl.vu.cs.ajira.data.types.TLongArray;
import nl.vu.cs.ajira.data.types.TStringArray;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.examples.trending_topics.tools.RankableString;

public final class TotalRankingsAction extends AbstractRankerAction {

	@Override
	void updateRankingsWithTuple(Tuple tuple) {
		String[] strArray = ((TStringArray) tuple.get(0)).getArray();
		long[] countArray = ((TLongArray) tuple.get(1)).getArray();
		assert (strArray.length == countArray.length);
		for (int i = 0; i < strArray.length; i++) {
			String str = strArray[i];
			long count = countArray[i];
			super.getRankings().updateWith(new RankableString(str, count));
		}
		super.getRankings().pruneZeroCounts();
	}

}
