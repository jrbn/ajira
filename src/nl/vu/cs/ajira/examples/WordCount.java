package nl.vu.cs.ajira.examples;

import nl.vu.cs.ajira.Ajira;
import nl.vu.cs.ajira.actions.Action;
import nl.vu.cs.ajira.actions.ActionConf;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionFactory;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.actions.ActionSequence;
import nl.vu.cs.ajira.actions.GroupBy;
import nl.vu.cs.ajira.actions.ReadFromFiles;
import nl.vu.cs.ajira.actions.WriteToFiles;
import nl.vu.cs.ajira.data.types.TBag;
import nl.vu.cs.ajira.data.types.TInt;
import nl.vu.cs.ajira.data.types.TLong;
import nl.vu.cs.ajira.data.types.TString;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.exceptions.ActionNotConfiguredException;
import nl.vu.cs.ajira.submissions.Job;
import nl.vu.cs.ajira.submissions.Submission;
import nl.vu.cs.ajira.utils.Configuration;
import nl.vu.cs.ajira.utils.Consts;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WordCount {

	static final Logger log = LoggerFactory.getLogger(WordCount.class);

	/**
	 * This action splits the text in input in a sequence of words. Each word is
	 * output as a tuple with 1 as associated counter.
	 * 
	 * @author Jacopo Urbani
	 * 
	 */
	public static class CountWords extends Action {
		@Override
		public void process(Tuple tuple, ActionContext context,
				ActionOutput actionOutput) throws Exception {
			TString iText = (TString) tuple.get(0);
			String sText = iText.getValue();
			if (sText != null && sText.length() > 0) {
				String[] words = sText.split("\\s+");
				for (String word : words) {
					if (word.length() > 0) {
						TString oWord = new TString(word);
						actionOutput.output(oWord, new TInt(1));
					}
				}
			}
		}
	}

	/**
	 * 
	 * This action simply sums up the counts for each word. The result is a pair
	 * of the form <word, count>
	 * 
	 * @author Jacopo Urbani
	 * 
	 */
	public static class SumCounts extends Action {
		@Override
		public void process(Tuple tuple, ActionContext context,
				ActionOutput actionOutput) throws Exception {
			TString word = (TString) tuple.get(0);
			TBag values = (TBag) tuple.get(1);

			long sum = 0;
			for (Tuple t : values) {
				sum += ((TInt) t.get(0)).getValue();
			}
			actionOutput.output(word, new TLong(sum));
		}
	}

	public static Job createJob(String inDir, String outDir)
			throws ActionNotConfiguredException {
		Job job = new Job();
		ActionSequence actions = new ActionSequence();

		// Read the input files
		ActionConf action = ActionFactory.getActionConf(ReadFromFiles.class);
		action.setParamString(ReadFromFiles.S_PATH, inDir);
		actions.add(action);

		// Count the words
		actions.add(ActionFactory.getActionConf(CountWords.class));

		// Groups the pairs
		action = ActionFactory.getActionConf(GroupBy.class);
		action.setParamStringArray(GroupBy.SA_TUPLE_FIELDS,
				TString.class.getName(), TInt.class.getName());
		action.setParamByteArray(GroupBy.IA_FIELDS_TO_GROUP, (byte) 0);
		actions.add(action);

		// Sum the counts
		actions.add(ActionFactory.getActionConf(SumCounts.class));

		// Write the results on files
		action = ActionFactory.getActionConf(WriteToFiles.class);
		action.setParamString(WriteToFiles.S_OUTPUT_DIR, outDir);
		actions.add(action);

		job.setActions(actions);
		return job;
	}

	/**
	 * Example program: The superfamous WordCount!
	 * 
	 * @param args
	 * @throws ActionNotConfiguredException
	 */
	public static void main(String[] args) {

		if (args.length < 2) {
			System.out.println("Usage: " + WordCount.class.getSimpleName()
					+ " <input directory> <output directory>");
			System.exit(1);
		}

		// Start up the cluster
		Ajira ajira = new Ajira();
		Configuration conf = ajira.getConfiguration();

		// Init some configuration params of the cluster
		conf.setInt(Consts.N_PROC_THREADS, 2);
		ajira.startup();

		// With this command we ensure that we submit the job only once
		if (ajira.amItheServer()) {

			// Configure the job and launch it!
			try {

				Job job = createJob(args[0], args[1]);
				Submission sub = ajira.waitForCompletion(job);
				sub.printStatistics();

			} catch (ActionNotConfiguredException e) {
				log.error("The job was not properly configured", e);
			}

			ajira.shutdown();

		}
	}
}
