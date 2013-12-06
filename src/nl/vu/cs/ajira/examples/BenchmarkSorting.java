package nl.vu.cs.ajira.examples;

import nl.vu.cs.ajira.Ajira;
import nl.vu.cs.ajira.actions.ActionConf;
import nl.vu.cs.ajira.actions.ActionFactory;
import nl.vu.cs.ajira.actions.ActionSequence;
import nl.vu.cs.ajira.actions.CollectToNode;
import nl.vu.cs.ajira.actions.PartitionToNodes;
import nl.vu.cs.ajira.actions.ReadFromFiles;
import nl.vu.cs.ajira.actions.RemoveDuplicates;
import nl.vu.cs.ajira.actions.WriteToFiles;
import nl.vu.cs.ajira.data.types.TString;
import nl.vu.cs.ajira.submissions.Job;
import nl.vu.cs.ajira.submissions.Submission;
import nl.vu.cs.ajira.utils.Configuration;
import nl.vu.cs.ajira.utils.Consts;

public class BenchmarkSorting {

	private static String output = "files";
	private static boolean ibis = false;
	private static int nProcThreads = 1;

	private static void parseArgs(String[] args) {
		for (int i = 2; i < args.length; ++i) {
			String param = args[i];

			if (param.equals("--ibis-server")) {
				ibis = true;
			}

			if (param.equals("--output")) {
				output = args[++i];
			}

			if (param.equals("--procs")) {
				nProcThreads = Integer.parseInt(args[++i]);
			}
		}
	}

	public static void main(String[] args) throws Exception {

		if (args.length < 2) {
			System.out
					.println("Usage: BenchmarkSorting <input dir> <output dir> --output [btree,files,none] --ibis-server --procs <num>");
			System.exit(0);
		}

		parseArgs(args);

		// Launch a simple job

		Ajira arch = new Ajira();
		Configuration conf = arch.getConfiguration();

		// Init some configuration params of the cluster
		conf.setBoolean(Consts.START_IBIS, ibis);
		conf.setInt(Consts.N_PROC_THREADS, nProcThreads);
		// conf.setInt(ReadFromFiles.MINIMUM_SPLIT_SIZE, 30000);

		// Start the cluster
		arch.startup();

		if (arch.amItheServer()) {
			// Now we can launch our program
			Job job = new Job();

			// Set up the program
			ActionSequence actions = new ActionSequence();

			// Split the input in more chunks, so that the reading
			// is done in parallel
			ActionConf c = ActionFactory.getActionConf(ReadFromFiles.class);
			c.setParamString(ReadFromFiles.S_PATH, args[0]);
			actions.add(c);

			// Distribute all the lines
			c = ActionFactory.getActionConf(PartitionToNodes.class);
			c.setParamStringArray(PartitionToNodes.SA_TUPLE_FIELDS,
					TString.class.getName());
			c.setParamBoolean(PartitionToNodes.B_SORT, true);
			int nNodes = arch.getNumberNodes();
			int nPartitionsPerNode = nNodes > 32 ? 1 : 32 / nNodes; // Assumes
																	// nNodes is
																	// a power
																	// of 2.
			c.setParamInt(PartitionToNodes.I_NPARTITIONS_PER_NODE,
					nPartitionsPerNode);
			actions.add(c);

			// Remove the duplicates
			actions.add(ActionFactory.getActionConf(RemoveDuplicates.class));

			if (output.equals("files")) {
				c = ActionFactory.getActionConf(WriteToFiles.class);
				c.setParamString(WriteToFiles.S_PATH, args[1]);
				actions.add(c);
			} else if (output.equals("btree")) {
				// TODO: Implement
			} else if (output.equals("none")) {
				c = ActionFactory.getActionConf(CollectToNode.class);
				c.setParamStringArray(CollectToNode.SA_TUPLE_FIELDS,
						TString.class.getName());
				actions.add(c);
			}

			// Launch it!
			job.setActions(actions);
			Submission s = arch.waitForCompletion(job);
			s.printStatistics();

			// Exit
			arch.shutdown();
			System.exit(0);

		}

	}
}
