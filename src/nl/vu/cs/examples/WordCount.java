package nl.vu.cs.examples;

import java.util.ArrayList;
import java.util.List;

import nl.vu.cs.ajira.Ajira;
import nl.vu.cs.ajira.actions.Action;
import nl.vu.cs.ajira.actions.ActionConf;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionFactory;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.actions.PartitionToNodes;
import nl.vu.cs.ajira.actions.ReadFromFiles;
import nl.vu.cs.ajira.actions.WriteToFiles;
import nl.vu.cs.ajira.data.types.TInt;
import nl.vu.cs.ajira.data.types.TString;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.submissions.Job;
import nl.vu.cs.ajira.submissions.Submission;

public class WordCount {

	/**
	 * This action splits the text in input in a sequence of words. Each word is
	 * output as a tuple with 1 as associated counter.
	 * 
	 * @author Jacopo Urbani
	 * 
	 */
	public static class CountWords extends Action {

		private final TString iText = new TString();
		private final TString oWord = new TString();
		private static final TInt oCount = new TInt(1);

		@Override
		public void process(Tuple tuple, ActionContext context,
				ActionOutput actionOutput) throws Exception {
			tuple.get(iText);
			String sText = iText.getValue();
			if (sText != null && sText.length() > 0) {
				String[] words = sText.split(" ");
				for (String word : words) {
					oWord.setValue(word);
					actionOutput.output(oWord, oCount);
				}
			}
		}
	}

	public static class SumCounts extends Action {

		private long sum;
		private String currentWord;
		private final TInt count = new TInt();
		private final TString word = new TString();

		@Override
		public void startProcess(ActionContext context) throws Exception {
			sum = 0;
			currentWord = null;
		}

		@Override
		public void process(Tuple tuple, ActionContext context,
				ActionOutput actionOutput) throws Exception {
			// TODO Auto-generated method stub

		}

	}

	/**
	 * Example program: The superfamous WordCount!
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		// Start up the cluster
		Ajira ajira = new Ajira();
		ajira.startup();

		// With this command we ensure that we submit the job only once
		if (ajira.amItheServer()) {

			// Configure the job
			Job job = new Job();
			List<ActionConf> actions = new ArrayList<>();

			// Read the input files
			ActionConf action = ActionFactory
					.getActionConf(ReadFromFiles.class);
			action.setParam(ReadFromFiles.PATH, args[0]);
			actions.add(action);

			// Count the words
			actions.add(ActionFactory.getActionConf(CountWords.class));

			// Partitions them across the cluster
			actions.add(ActionFactory.getActionConf(PartitionToNodes.class));

			// Sum the counts
			actions.add(ActionFactory.getActionConf(SumCounts.class));

			// Write the results on files
			action = ActionFactory.getActionConf(WriteToFiles.class);
			action.setParam(WriteToFiles.OUTPUT_DIR, args[1]);

			// Launch it!
			Submission sub = ajira.waitForCompletion(job);

			// Print some statistics
			System.out.println("Execution time: " + sub.getExecutionTimeInMs()
					+ " ms.");

			// Shutdown the cluster
			ajira.shutdown();
		}
	}
}
