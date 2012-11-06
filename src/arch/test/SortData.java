package arch.test;

import java.util.ArrayList;
import java.util.List;

import arch.Arch;
import arch.actions.Action;
import arch.actions.SendTo;
import arch.actions.files.FileSplitter;
import arch.actions.files.FilterHiddenFiles;
import arch.actions.files.WriteToFile;
import arch.chains.Chain;
import arch.data.types.TInt;
import arch.data.types.TString;
import arch.data.types.Tuple;
import arch.datalayer.files.FilesLayer;
import arch.datalayer.files.LineTextFilesReader;
import arch.storage.TupleComparator;
import arch.submissions.JobDescriptor;
import arch.utils.Configuration;
import arch.utils.Consts;

public class SortData {

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {

		if (args.length != 3) {
			System.out
					.println("Usage: SortData <input dir> <output dir> <number partitions>");
			System.exit(0);
		}

		// TODO: still need to fix SendTo and store it in a B+Tree.

		// Launch a simple job
		Configuration conf = new Configuration();
		arch.Arch arch = new Arch();

		// Init some configuration params of the cluster
		conf.set(Consts.STORAGE_IMPL, FilesLayer.class.getName());
		conf.set(FilesLayer.IMPL_FILE_READER,
				LineTextFilesReader.class.getName());
		conf.setBoolean(Consts.START_IBIS, true);

		// Start the cluster
		arch.startup(conf);

		if (arch.isServer()) {
			// Now we can launch our program
			JobDescriptor job = new JobDescriptor();

			// Set up the program
			Chain chain = job.getNewChain();

			List<Action> actions = new ArrayList<Action>();

			// Split the input in more chunks, so that the reading
			// is done in parallel
			FileSplitter splitter = new FileSplitter();
			splitter.setInputTuple(new Tuple(new TInt(FilesLayer.OP_LS),
					new TString(args[0]), new TString(FilterHiddenFiles.class
							.getName())));
			actions.add(splitter);

			// Assign each line read in the files to a node in our pool
			LinePartitioner partitioner = new LinePartitioner();
			partitioner.setNumberPartitions(Integer.valueOf(args[2]));
			actions.add(partitioner);

			// Distribute all the lines
			SendTo sendTo = new SendTo();
			sendTo.setDestination(SendTo.MULTIPLE);
			sendTo.setBucketId(0);
			sendTo.setSortingFunction(TupleComparator.class.getName());
			actions.add(sendTo);

			// Store the output in new files
			WriteToFile writeToFile = new WriteToFile();
			writeToFile.setOutputDirectory(args[1]);
			actions.add(writeToFile);

			// Launch it!
			chain.addActions(actions);
			job.setMainChain(chain);
			arch.waitForCompletion(job);

			// Exit
			arch.shutdown();
			System.exit(0);

		}

	}
}
