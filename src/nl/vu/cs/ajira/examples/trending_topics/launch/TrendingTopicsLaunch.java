package nl.vu.cs.ajira.examples.trending_topics.launch;

import nl.vu.cs.ajira.Ajira;
import nl.vu.cs.ajira.actions.ActionConf;
import nl.vu.cs.ajira.actions.ActionFactory;
import nl.vu.cs.ajira.actions.ActionSequence;
import nl.vu.cs.ajira.actions.CollectToNode;
import nl.vu.cs.ajira.actions.PartitionToNodes;
import nl.vu.cs.ajira.data.types.TLong;
import nl.vu.cs.ajira.data.types.TLongArray;
import nl.vu.cs.ajira.data.types.TString;
import nl.vu.cs.ajira.data.types.TStringArray;
import nl.vu.cs.ajira.datalayer.InputLayer;
import nl.vu.cs.ajira.examples.trending_topics.actions.AbstractRankerAction;
import nl.vu.cs.ajira.examples.trending_topics.actions.RollingCountAction;
import nl.vu.cs.ajira.examples.trending_topics.io.RandomGeneratorInputLayer;
import nl.vu.cs.ajira.examples.trending_topics.io.RandomTupleGeneratorAction;
import nl.vu.cs.ajira.examples.trending_topics.io.TuplePrinter;
import nl.vu.cs.ajira.exceptions.ActionNotConfiguredException;
import nl.vu.cs.ajira.submissions.Job;
import nl.vu.cs.ajira.submissions.Submission;
import nl.vu.cs.ajira.utils.Consts;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TrendingTopicsLaunch {
	private static final Logger log = LoggerFactory
			.getLogger(TrendingTopicsLaunch.class);
	private static final boolean evalMode = true;

	private static int numThreads = 8;
	private static int numInputPartitionsPerNode = 2;
	private static int numIntermediatePartitionsPerNode = 2;

	// -1 Continues to produce tuples indefinitely
	private static int numTuples = -1; // 1000000;
	private static final int numDistinctWords = 1000;
	private static final int seed = 1;

	private static final int winSizeInSeconds = 10;
	private static final int updateFreqInSeconds = 1;
	private static final int topN = 10;

	public static Job createJob(String filename)
			throws ActionNotConfiguredException {
		Job job = new Job();
		ActionSequence actions = new ActionSequence();

		// Input
		RandomTupleGeneratorAction.addToChain(numTuples, numDistinctWords,
				seed, actions);

		// Partition by words
		ActionConf action = ActionFactory.getActionConf(PartitionToNodes.class);
		action.setParamInt(PartitionToNodes.I_NPARTITIONS_PER_NODE,
				numInputPartitionsPerNode);
		action.setParamBoolean(PartitionToNodes.B_SORT, false);
		action.setParamBoolean(PartitionToNodes.B_STREAMING, true);
		byte[] bytes = { 0 };
		action.setParamByteArray(PartitionToNodes.BA_PARTITION_FIELDS, bytes);
		action.setParamStringArray(PartitionToNodes.SA_TUPLE_FIELDS,
				TString.class.getName());
		actions.add(action);

		// Rolling count
		RollingCountAction.addToChain(winSizeInSeconds, updateFreqInSeconds,
				evalMode, actions);

		// Partition by words
		action = ActionFactory.getActionConf(PartitionToNodes.class);
		action.setParamInt(PartitionToNodes.I_NPARTITIONS_PER_NODE,
				numIntermediatePartitionsPerNode);
		action.setParamBoolean(PartitionToNodes.B_SORT, false);
		action.setParamBoolean(PartitionToNodes.B_STREAMING, true);
		action.setParamByteArray(PartitionToNodes.BA_PARTITION_FIELDS, bytes);
		action.setParamStringArray(PartitionToNodes.SA_TUPLE_FIELDS,
				TString.class.getName(), TLong.class.getName());
		actions.add(action);

		// Intermediate ranking
		AbstractRankerAction.addIntermediateRankingToChain(topN,
				updateFreqInSeconds, evalMode, actions);

		// Collect to node
		action = ActionFactory.getActionConf(CollectToNode.class);
		action.setParamBoolean(CollectToNode.B_SORT, false);
		action.setParamBoolean(CollectToNode.B_STREAMING_MODE, true);
		action.setParamStringArray(CollectToNode.SA_TUPLE_FIELDS,
				TStringArray.class.getName(), TLongArray.class.getName());
		actions.add(action);

		// Total ranking
		AbstractRankerAction.addTotalRankingToChain(topN, updateFreqInSeconds,
				evalMode, actions);

		// Print the results
		TuplePrinter.addToChain(filename, actions);

		job.setActions(actions);
		return job;
	}

	public static void main(String[] args) {
		if (args.length < 1) {
			System.err
					.println("Usage: TrendingTopicsLaunch <filename> -numThreads=<num_threads> -numInputPartitionsPerNode=<num_partitions_per_node> numIntermediatePartitionsPerNode=<num_partitions_per_node> -numTuples=<num_tuples>");
			System.exit(-1);
		}
		String filename = args[0];
		for (int i = 1; i < args.length; i++) {
			String[] tmp = args[i].split("=");
			if (tmp[0].equalsIgnoreCase("-threads")) {
				numThreads = Integer.parseInt(tmp[1]);
			} else if (tmp[0].equalsIgnoreCase("-numInputPartitionsPerNode")) {
				numInputPartitionsPerNode = Integer.parseInt(tmp[1]);
			} else if (tmp[0]
					.equalsIgnoreCase("-numIntermediatePartitionsPerNode")) {
				numIntermediatePartitionsPerNode = Integer.parseInt(tmp[1]);
			} else if (tmp[0].equalsIgnoreCase("-numTuples")) {
				numTuples = Integer.parseInt(tmp[1]);
			}
		}

		Ajira ajira = new Ajira();
		ajira.getConfiguration().set(InputLayer.INPUT_LAYER_CLASS,
				RandomGeneratorInputLayer.class.getName());
		ajira.getConfiguration().setInt(Consts.N_PROC_THREADS, numThreads);
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
