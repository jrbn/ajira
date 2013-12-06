package nl.vu.cs.ajira.examples.streaming_word_count.actions;

import java.util.StringTokenizer;

import nl.vu.cs.ajira.actions.Action;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.data.types.TString;
import nl.vu.cs.ajira.data.types.Tuple;

public class CountWords extends Action {
	@Override
	public void process(Tuple tuple, ActionContext context,
			ActionOutput actionOutput) throws Exception {
		TString iText = (TString) tuple.get(0);
		String sText = iText.getValue();
		StringTokenizer tok = new StringTokenizer(sText);
		while (tok.hasMoreTokens()) {
			String word = tok.nextToken();
			TString oWord = new TString(word);
			actionOutput.output(oWord);
		}
	}
}