package nl.vu.cs.ajira.examples.streaming_word_count.launch;

import nl.vu.cs.ajira.Ajira;
import nl.vu.cs.ajira.actions.ActionConf;
import nl.vu.cs.ajira.actions.ActionFactory;
import nl.vu.cs.ajira.actions.ActionSequence;
import nl.vu.cs.ajira.actions.CollectToNode;
import nl.vu.cs.ajira.actions.PartitionToNodes;
import nl.vu.cs.ajira.data.types.TLong;
import nl.vu.cs.ajira.data.types.TString;
import nl.vu.cs.ajira.examples.streaming_word_count.actions.CountWords;
import nl.vu.cs.ajira.examples.streaming_word_count.actions.SumCounts;
import nl.vu.cs.ajira.examples.streaming_word_count.io.RandomTupleGeneratorAction;
import nl.vu.cs.ajira.examples.streaming_word_count.io.TuplePrinter;
import nl.vu.cs.ajira.exceptions.ActionNotConfiguredException;
import nl.vu.cs.ajira.submissions.Job;
import nl.vu.cs.ajira.submissions.Submission;
import nl.vu.cs.ajira.utils.Consts;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamingWordCount {
	private static final Logger log = LoggerFactory
			.getLogger(StreamingWordCount.class);

	private static int numThreads = -1; // means that Ajira will determine this.
	private static int numPartitions = 4;

	// -1 Continues to produce tuples indefinitely
	private static int numTuples = -1;

	private static final int numDistinctPhrases = 100;
	private static final int numWordsPerPhrase = 10;
	private static final int numDistinctWords = 100;
	private static final int seed = 0;

	public static Job createJob(String filename)
			throws ActionNotConfiguredException {
		Job job = new Job();
		ActionSequence actions = new ActionSequence();

		// Input
		RandomTupleGeneratorAction.addToChain(numTuples, numDistinctPhrases,
				numWordsPerPhrase, numDistinctWords, seed, actions);

		// Count the words
		actions.add(ActionFactory.getActionConf(CountWords.class));

		ActionConf action = ActionFactory.getActionConf(PartitionToNodes.class);
		action.setParamInt(PartitionToNodes.I_NPARTITIONS_PER_NODE,
				numPartitions);
		action.setParamBoolean(PartitionToNodes.B_SORT, false);
		action.setParamBoolean(PartitionToNodes.B_STREAMING, true);
		byte[] bytes = { 0 };
		action.setParamByteArray(PartitionToNodes.BA_PARTITION_FIELDS, bytes);
		action.setParamStringArray(PartitionToNodes.SA_TUPLE_FIELDS,
				TString.class.getName());
		actions.add(action);

		// Sum the counts
		actions.add(ActionFactory.getActionConf(SumCounts.class));

		// Collect to node
		action = ActionFactory.getActionConf(CollectToNode.class);
		action.setParamBoolean(CollectToNode.B_SORT, false);
		action.setParamBoolean(CollectToNode.B_STREAMING_MODE, true);
		action.setParamStringArray(CollectToNode.SA_TUPLE_FIELDS,
				TString.class.getName(), TLong.class.getName());
		actions.add(action);

		// Print the results
		TuplePrinter.addToChain(filename, actions);

		job.setActions(actions);
		return job;
	}

	public static void main(String[] args) {
		if (args.length < 1) {
			System.err
					.println("Usage: StreamingWordCount <filename> -numThreads=<num_threads> -numPartitionsPerNode=<num_partitions_per_node> -numTuples=<num_tuples>");
			System.exit(-1);
		}
		String filename = args[0];
		for (int i = 1; i < args.length; i++) {
			String[] tmp = args[i].split("=");
			if (tmp[0].equalsIgnoreCase("-threads")) {
				numThreads = Integer.parseInt(tmp[1]);
			} else if (tmp[0].equalsIgnoreCase("-numPartitionsPerNode")) {
				numPartitions = Integer.parseInt(tmp[1]);
			} else if (tmp[0].equalsIgnoreCase("-numTuples")) {
				numTuples = Integer.parseInt(tmp[1]);
			}
		}

		Ajira ajira = new Ajira();
		if (numThreads > 0) {
			ajira.getConfiguration().setInt(Consts.N_PROC_THREADS, numThreads);
		}
		try {
			ajira.startup();
		} catch (Exception e) {
			e.printStackTrace();
		}
		if (ajira.amItheServer()) {
			try {
				Job job = createJob(filename);
				Submission sub = ajira.waitForCompletion(job);
				sub.printStatistics();
				if (sub.getState().equals(Consts.STATE_FAILED)) {
					log.error("The job failed", sub.getException());
				}
			} catch (ActionNotConfiguredException e) {
				log.error("The job was not properly configured", e);
			} finally {
				ajira.shutdown();
			}
		}
	}
}
