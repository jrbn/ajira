package nl.vu.cs.ajira.examples.streaming_word_count.actions;

import java.util.HashMap;
import java.util.Map;

import nl.vu.cs.ajira.actions.Action;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.data.types.TLong;
import nl.vu.cs.ajira.data.types.TString;
import nl.vu.cs.ajira.data.types.Tuple;

public class SumCounts extends Action {
	private final Map<String, Long> wordMap = new HashMap<String, Long>();

	@Override
	public void process(Tuple tuple, ActionContext context,
			ActionOutput actionOutput) throws Exception {
		TString word = (TString) tuple.get(0);
		String wordString = word.getValue();
		Long currentCount = wordMap.get(wordString);
		if (currentCount == null) {
			currentCount = 1L;
		} else {
			currentCount++;
		}
		wordMap.put(wordString, currentCount);
		actionOutput.output(word, new TLong(currentCount));
	}
}
