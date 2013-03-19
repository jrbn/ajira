package nl.vu.cs.ajira.examples;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import nl.vu.cs.ajira.Ajira;
import nl.vu.cs.ajira.AjiraClient;
import nl.vu.cs.ajira.actions.Action;
import nl.vu.cs.ajira.actions.ActionConf;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionFactory;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.actions.GroupBy;
import nl.vu.cs.ajira.actions.ReadFromFiles;
import nl.vu.cs.ajira.actions.WriteToFiles;
import nl.vu.cs.ajira.data.types.TBag;
import nl.vu.cs.ajira.data.types.TInt;
import nl.vu.cs.ajira.data.types.TLong;
import nl.vu.cs.ajira.data.types.TString;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.submissions.Job;
import nl.vu.cs.ajira.submissions.JobFailedException;
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
	
	public static Job createJob(String inDir, String outDir) {
		Job job = new Job();
		List<ActionConf> actions = new ArrayList<ActionConf>();

		// Read the input files
		ActionConf action = ActionFactory
				.getActionConf(ReadFromFiles.class);
		action.setParamString(ReadFromFiles.S_PATH, inDir);
		actions.add(action);

		// Count the words
		actions.add(ActionFactory.getActionConf(CountWords.class));

		// Groups the pairs
		action = ActionFactory.getActionConf(GroupBy.class);
		action.setParamStringArray(GroupBy.TUPLE_FIELDS,
				TString.class.getName(), TInt.class.getName());
		action.setParamByteArray(GroupBy.FIELDS_TO_GROUP, (byte) 0);
		actions.add(action);

		// Sum the counts
		actions.add(ActionFactory.getActionConf(SumCounts.class));

		// Write the results on files
		action = ActionFactory.getActionConf(WriteToFiles.class);
		action.setParamString(WriteToFiles.OUTPUT_DIR, outDir);
		actions.add(action);

		job.setActions(actions);
		return job;
	}
	

	/**
	 * Example program: The superfamous WordCount!
	 * 
	 * @param args
	 */
	public static void main(String[] args) {

		if (args.length < 2) {
			System.out.println("Usage: " + WordCount.class.getSimpleName()
					+ " <input directory> <output directory>");
			System.exit(1);
		}
		
		if (args.length > 2) {
			// Third argument is cluster file.
			Job job = createJob(args[0], args[1]);
			AjiraClient client = null;
			try {
				client = new AjiraClient(args[2], job);
			} catch (IOException e) {
				System.err.println("Could not create Ajira client");
				e.printStackTrace(System.err);
				System.exit(1);
			}
			List<Tuple> result = new ArrayList<Tuple>();
			try {
				client.getResult(result);
			} catch (JobFailedException e) {
				System.err.println("Job failed");
				e.printStackTrace(System.err);
				System.exit(1);
			}
			return;
		}

		// Start up the cluster
		Ajira ajira = new Ajira();
		ajira.startup();

		// With this command we ensure that we submit the job only once
		if (ajira.amItheServer()) {

			// Configure the job
			Job job = createJob(args[0], args[1]);
			
			// Launch it!
			Submission sub;
			try {
				sub = ajira.waitForCompletion(job);
				// Print output
				sub.printStatistics();
			} catch (JobFailedException e) {
				System.err.println("Job failed: " + e);
				e.printStackTrace(System.err);
				Throwable ex = e.getCause();
				while (ex != null) {
					System.err.println("Caused by " + ex);
					ex.printStackTrace(System.err);
					ex = e.getCause();
				}
			} finally {
				// Shutdown the cluster
				ajira.shutdown();
			}

		}
	}
}
