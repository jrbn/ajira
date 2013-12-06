package nl.vu.cs.ajira.examples.trending_topics.actions;

import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.examples.trending_topics.tools.RankableString;

public final class IntermediateRankingsAction extends AbstractRankerAction {

	@Override
	void updateRankingsWithTuple(Tuple tuple) {
		RankableString rankable = RankableString.from(tuple);
		super.getRankings().updateWith(rankable);
	}

}
