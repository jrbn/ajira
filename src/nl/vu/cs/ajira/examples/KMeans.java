package nl.vu.cs.ajira.examples;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import nl.vu.cs.ajira.Ajira;
import nl.vu.cs.ajira.actions.Action;
import nl.vu.cs.ajira.actions.ActionConf;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionFactory;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.actions.ActionSequence;
import nl.vu.cs.ajira.actions.CollectToNode;
import nl.vu.cs.ajira.actions.GroupBy;
import nl.vu.cs.ajira.actions.ReadFromBucket;
import nl.vu.cs.ajira.actions.ReadFromFiles;
import nl.vu.cs.ajira.actions.WriteToBucket;
import nl.vu.cs.ajira.actions.WriteToFiles;
import nl.vu.cs.ajira.data.types.TBag;
import nl.vu.cs.ajira.data.types.TDoubleArray;
import nl.vu.cs.ajira.data.types.TInt;
import nl.vu.cs.ajira.data.types.TString;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.exceptions.ActionNotConfiguredException;
import nl.vu.cs.ajira.submissions.Job;
import nl.vu.cs.ajira.submissions.Submission;
import nl.vu.cs.ajira.utils.Consts;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KMeans {

	static final Logger log = LoggerFactory.getLogger(KMeans.class);

	private static final String CENTERS = "KMeansMapper.centers";
	private static final String NEW_CENTERS = "KMeansMapper.newcenters";
	private static final int MAX_NUMBER = 100;
	private static final int PARTITIONS_PER_NODE = 4;
	private static final int CONVERGENCE_THRESHOLD = Integer.valueOf(System
			.getProperty("convergence_threshold"));

	/***** SUPPORT METHODS *****/
	private static final double eucledianDistance(double[] set1, double[] set2) {
		double sum = 0;
		int length = set1.length;
		for (int i = 0; i < length; i++) {
			double diff = set2[i] - set1[i];
			sum += (diff * diff);
		}
		return Math.sqrt(sum);
	}

	private static final void sum(double[] s1, double[] s2) {
		for (int i = 0; i < s1.length; ++i) {
			long el1 = Math.round(s1[i] * 100);
			long el2 = Math.round(s2[i] * 100);
			s1[i] = (el1 + el2) / 100.0;
		}
	}

	private static final void divide(double[] s1, int s) {
		for (int i = 0; i < s1.length; ++i) {
			s1[i] /= s;
			// We round it to two decimals
			s1[i] = (double) Math.round(s1[i] * 100) / 100;
		}
	}

	private static final void kmeans(ActionSequence actions, String outputDir)
			throws ActionNotConfiguredException {
		// Find the closest center
		actions.add(ActionFactory.getActionConf(FindClosestCenter.class));

		// Groups the pairs
		ActionConf action = ActionFactory.getActionConf(GroupBy.class);
		action.setParamStringArray(GroupBy.SA_TUPLE_FIELDS,
				TInt.class.getName(), TDoubleArray.class.getName());
		action.setParamByteArray(GroupBy.BA_FIELDS_TO_GROUP, (byte) 0);
		action.setParamInt(GroupBy.I_NPARTITIONS_PER_NODE, PARTITIONS_PER_NODE);
		actions.add(action);

		// Update the clusters
		actions.add(ActionFactory.getActionConf(UpdateClusters.class));

		// Write the results in a bucket
		action = ActionFactory.getActionConf(WriteToBucket.class);
		action.setParamStringArray(WriteToBucket.SA_TUPLE_FIELDS,
				TInt.class.getName(), TDoubleArray.class.getName());
		actions.add(action);

		// Collect to one node
		action = ActionFactory.getActionConf(CollectToNode.class);
		action.setParamStringArray(CollectToNode.SA_TUPLE_FIELDS,
				TInt.class.getName(), TInt.class.getName());
		actions.add(action);

		// Update the centroids
		action = ActionFactory.getActionConf(UpdateCentroids.class);
		action.setParamString(UpdateCentroids.S_OUTPUT_DIR, outputDir);
		actions.add(action);
	}

	private static final double[] parseVector(String line) {
		String[] numbers = line.split(" ");
		double[] output = new double[numbers.length];
		for (int i = 0; i < output.length; ++i) {
			output[i] = Double.valueOf(numbers[i]);
		}
		return output;
	}

	private static final void generateInput(int sizeVector,
			int nVectorsPerFile, int nFiles, String outputDir, int nCenters,
			String centerFile) throws IOException {
		Random r = new Random();
		DecimalFormat df = new DecimalFormat("#####0.00");

		File fOutputDir = new File(outputDir);
		if (fOutputDir.exists()) {
			// Delete each file and remove the directory
			for (File f : fOutputDir.listFiles()) {
				f.delete();
			}
			fOutputDir.delete();
		}
		fOutputDir.mkdir();

		for (int idxFile = 0; idxFile < nFiles; ++idxFile) {
			FileWriter f = new FileWriter(new File(outputDir + "/" + idxFile));
			for (int nRow = 0; nRow < nVectorsPerFile; ++nRow) {
				String line = "";
				for (int j = 0; j < sizeVector; ++j) {
					double n = Math.abs(r.nextDouble() * MAX_NUMBER);
					line += df.format(n) + " ";
				}
				line = line.trim() + "\n";
				f.write(line);
			}
			f.close();
		}

		File fCenter = new File(centerFile);
		FileWriter writerCenter = new FileWriter(fCenter, false);
		for (int i = 0; i < nCenters; ++i) {
			String line = "";
			for (int j = 0; j < sizeVector; ++j) {
				double n = Math.abs(r.nextDouble() * MAX_NUMBER);
				line += df.format(n) + " ";
			}
			line = line.trim() + "\n";
			writerCenter.write(line);
		}
		writerCenter.close();
	}

	/***** END SUPPORT METHODS *****/

	/*
	 * Read the centers from a file and put them in an in-memory data structure
	 */
	public static class ReadCentroidsAndStartKMeans extends Action {
		public static final int S_INPUT_DIR = 0;
		public static final int S_OUTPUT_DIR = 1;

		private Map<Integer, double[]> centers;
		private String inputDir;
		private String outputDir;
		int counter;

		@Override
		protected void registerActionParameters(ActionConf conf) {
			conf.registerParameter(S_INPUT_DIR, "", null, true);
			conf.registerParameter(S_OUTPUT_DIR, "", null, true);
		}

		@Override
		public void startProcess(ActionContext context) throws Exception {
			centers = new HashMap<Integer, double[]>();
			inputDir = getParamString(S_INPUT_DIR);
			outputDir = getParamString(S_OUTPUT_DIR);
			counter = 0;
		}

		@Override
		public void process(Tuple tuple, ActionContext context,
				ActionOutput actionOutput) throws Exception {
			String line = ((TString) tuple.get(0)).getValue();
			centers.put(counter++, parseVector(line));
		}

		@Override
		public void stopProcess(ActionContext context, ActionOutput actionOutput)
				throws Exception {
			context.putObjectInCache(CENTERS, centers);
			context.broadcastCacheObjects(CENTERS);

			if (context.isPrincipalBranch()) {
				ActionSequence actions = new ActionSequence();

				// Read the input files
				ActionConf action = ActionFactory
						.getActionConf(ReadFromFiles.class);
				action.setParamString(ReadFromFiles.S_PATH, inputDir);
				actions.add(action);

				// Parse the textual lines into vectors
				actions.add(ActionFactory.getActionConf(ParseVectors.class));

				// Start the k-means procedure with a branch
				kmeans(actions, outputDir);
				actionOutput.branch(actions);
			}

			inputDir = null;
			outputDir = null;
			centers = null;
		}
	}

	/* Parse a vector encodend in a string as a sequence of numbers */
	public static class ParseVectors extends Action {

		private final TDoubleArray array = new TDoubleArray();

		@Override
		public void process(Tuple tuple, ActionContext context,
				ActionOutput actionOutput) throws Exception {
			String line = ((TString) tuple.get(0)).getValue();
			double[] vector = parseVector(line);
			array.setArray(vector);
			actionOutput.output(array);
		}
	}

	/* For each vector, find the closest center */
	public static class FindClosestCenter extends Action {

		private Set<Map.Entry<Integer, double[]>> centers = null;
		private final TInt key = new TInt();

		@SuppressWarnings("unchecked")
		@Override
		public void startProcess(ActionContext context) throws Exception {
			centers = ((Map<Integer, double[]>) context
					.getObjectFromCache(CENTERS)).entrySet();
		}

		@Override
		public void process(Tuple tuple, ActionContext context,
				ActionOutput actionOutput) throws Exception {
			TDoubleArray vector = (TDoubleArray) tuple
					.get(tuple.getNElements() - 1);
			double[] nearest = null;
			double nearestDistance = Double.MAX_VALUE;
			int index = -1;
			for (Map.Entry<Integer, double[]> c : centers) {
				double dist = eucledianDistance(c.getValue(), vector.getArray());
				if (nearest == null) {
					nearest = c.getValue();
					nearestDistance = dist;
					index = c.getKey();
				} else {
					if (nearestDistance > dist) {
						nearest = c.getValue();
						nearestDistance = dist;
						index = c.getKey();
					}
				}
			}

			key.setValue(index);
			actionOutput.output(key, vector);
		}

		@Override
		public void stopProcess(ActionContext context, ActionOutput actionOutput)
				throws Exception {
			centers = null;
		}

	}

	public static class UpdateClusters extends Action {

		private final TInt key = new TInt();
		private final TDoubleArray value = new TDoubleArray();
		private Map<Integer, double[]> newCenters = null;

		@Override
		public void startProcess(ActionContext context) throws Exception {
			newCenters = new HashMap<Integer, double[]>();
		}

		@Override
		public void process(Tuple tuple, ActionContext context,
				ActionOutput actionOutput) throws Exception {

			List<double[]> vectorList = new ArrayList<double[]>();
			double[] newCenter = null;
			TBag values = (TBag) tuple.get(1);
			for (Tuple value : values) {
				double[] aValue = ((TDoubleArray) value.get(0)).getArray();
				aValue = Arrays.copyOf(aValue, aValue.length);
				vectorList.add(aValue);
				if (newCenter == null) {
					newCenter = Arrays.copyOf(aValue, aValue.length);
				} else {
					sum(newCenter, aValue);
				}
			}
			divide(newCenter, vectorList.size());

			int groupk = ((TInt) tuple.get(0)).getValue();
			key.setValue(groupk);
			for (double[] vector : vectorList) {
				value.setArray(vector);
				actionOutput.output(key, value);
			}

			// Add in an in-memory data structure the new centers
			newCenters.put(groupk, newCenter);
		}

		@Override
		public void stopProcess(ActionContext context, ActionOutput actionOutput)
				throws Exception {
			// Add the new center in main memory. The synchronization is
			// necessary
			// because multiple threads could access the same memory.
			synchronized (UpdateClusters.class) {
				@SuppressWarnings("unchecked")
				Map<Integer, double[]> existingMap = (Map<Integer, double[]>) context
						.getObjectFromCache(NEW_CENTERS);
				if (existingMap != null) {
					existingMap.putAll(newCenters);
				} else {
					context.putObjectInCache(NEW_CENTERS, newCenters);
				}
			}
			newCenters = null;
		}
	}

	/* Replace center index with actual center. */
	public static class ReplaceIndex extends Action {
		private Map<Integer, double[]> centers;
		private final TDoubleArray center = new TDoubleArray();

		@SuppressWarnings("unchecked")
		@Override
		public void startProcess(ActionContext context) throws Exception {
			centers = (Map<Integer, double[]>) context
					.getObjectFromCache(CENTERS);
		}

		@Override
		public void process(Tuple tuple, ActionContext context,
				ActionOutput actionOutput) throws Exception {
			int index = ((TInt) tuple.get(0)).getValue();
			center.setArray(centers.get(index));
			actionOutput.output(center, tuple.get(1));
		}
	}

	/* Read the centers and put them in a list */
	public static class UpdateCentroids extends Action {

		public static final int S_OUTPUT_DIR = 0;
		private String outputDir;
		private Map<Integer, Integer> buckets;

		@Override
		protected void registerActionParameters(ActionConf conf) {
			conf.registerParameter(S_OUTPUT_DIR, "", null, true);
		}

		@Override
		public void startProcess(ActionContext context) throws Exception {
			outputDir = getParamString(S_OUTPUT_DIR);
			buckets = new HashMap<Integer, Integer>();
		}

		@Override
		public void process(Tuple tuple, ActionContext context,
				ActionOutput actionOutput) throws Exception {
			buckets.put(((TInt) tuple.get(1)).getValue(),
					((TInt) tuple.get(0)).getValue());
		}

		@SuppressWarnings("unchecked")
		@Override
		public void stopProcess(ActionContext context, ActionOutput actionOutput)
				throws Exception {
			context.incrCounter("K-Means iterations", 1);
			long distance = 0;

			Map<Integer, double[]> centersPreviousIteration = (Map<Integer, double[]>) context
					.getObjectFromCache(CENTERS);
			Map<Integer, double[]> localCenters = (Map<Integer, double[]>) context
					.getObjectFromCache(NEW_CENTERS);
			List<Object[]> otherCenters = context
					.retrieveCacheObjects(NEW_CENTERS);
			Map<Integer, double[]> centersForNextIteration = new HashMap<Integer, double[]>();

			// Check if they are changed or not
			// System.out.println("Centers for next iteration: ");
			for (Map.Entry<Integer, double[]> pair : localCenters.entrySet()) {

				double d = eucledianDistance(pair.getValue(),
						centersPreviousIteration.get(pair.getKey()));
				long dd = Math.round(d * 100);
				distance += dd / 100;

				centersForNextIteration.put(pair.getKey(), pair.getValue());
			}

			// Also check the other centers
			if (otherCenters != null) {
				for (Object[] oCenters : otherCenters) {
					Map<Integer, double[]> centers = (Map<Integer, double[]>) oCenters[0];
					for (Map.Entry<Integer, double[]> pair : centers.entrySet()) {

						double d = eucledianDistance(pair.getValue(),
								centersPreviousIteration.get(pair.getKey()));
						long dd = Math.round(d * 100);
						distance += dd / 100;

						centersForNextIteration.put(pair.getKey(),
								pair.getValue());
					}
				}
			}

			boolean changed = distance > CONVERGENCE_THRESHOLD;
			if (changed) {
				// Update the new centers
				context.putObjectInCache(NEW_CENTERS, null);
				context.putObjectInCache(CENTERS, centersForNextIteration);
				context.broadcastCacheObjects(CENTERS, NEW_CENTERS);
			}

			int[] nodeIds = new int[buckets.size()];
			int[] bucketIds = new int[buckets.size()];
			int i = 0;
			for (Map.Entry<Integer, Integer> entry : buckets.entrySet()) {
				nodeIds[i] = entry.getValue();
				bucketIds[i] = entry.getKey();
				i++;
			}

			ActionSequence actions = new ActionSequence();
			ActionConf action = ActionFactory
					.getActionConf(ReadFromBucket.class);

			action.setParamIntArray(ReadFromBucket.IA_BUCKET_IDS, bucketIds);
			action.setParamIntArray(ReadFromBucket.IA_NODE_IDS, nodeIds);
			actions.add(action);

			if (changed) {
				// Restart the cycle
				kmeans(actions, outputDir);
			} else {
				// Write everything to files
				action = ActionFactory.getActionConf(ReplaceIndex.class);
				actions.add(action);
				action = ActionFactory.getActionConf(WriteToFiles.class);
				action.setParamString(WriteToFiles.S_PATH, outputDir);
				actions.add(action);
			}
			actionOutput.branch(actions);
		}
	}

	/**
	 * Example program: K-Means calculator
	 * 
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {

		if (args.length < 2) {
			System.out.println("Usage: " + KMeans.class.getSimpleName()
					+ " launch <input directory>" + " <initial centers file>"
					+ " <output directory>");
			System.out.println("OR");
			System.out.println("Usage: " + KMeans.class.getSimpleName()
					+ " generateInput <output directory> "
					+ "<size of the vector> " + "<n. files> "
					+ "<vectorsPerFile> <n centers> <file center>");
			System.exit(1);
		}

		if (args[0].equals("generateInput")) {
			String outputDir = args[1];
			int vectorSize = Integer.valueOf(args[2]);
			int nFiles = Integer.valueOf(args[3]);
			int vectorsPerFile = Integer.valueOf(args[4]);
			int nCenters = Integer.valueOf(args[5]);
			String fileCenter = args[6];

			generateInput(vectorSize, vectorsPerFile, nFiles, outputDir,
					nCenters, fileCenter);

		} else if (args[0].equals("launch")) {

			// Start up the cluster
			Ajira ajira = new Ajira();
			try {
				ajira.startup();
			} catch (Throwable e) {
				log.error("Could not start up Ajira", e);
				System.exit(1);
			}

			// With this command we ensure that we submit the job only once
			if (ajira.amItheServer()) {

				// Configure the job and launch it!
				try {
					String inDir = args[1];
					String centerFile = args[2];
					String outDir = args[3];

					Job job = new Job();
					ActionSequence actions = new ActionSequence();

					// Read the file with the centers
					ActionConf action = ActionFactory
							.getActionConf(ReadFromFiles.class);
					action.setParamString(ReadFromFiles.S_PATH, centerFile);
					actions.add(action);

					// More files could be read. Let's channel everything in a
					// single flow
					action = ActionFactory.getActionConf(CollectToNode.class);
					action.setParamStringArray(CollectToNode.SA_TUPLE_FIELDS,
							TString.class.getName());
					actions.add(action);

					// Read the centers and launch the computation
					action = ActionFactory
							.getActionConf(ReadCentroidsAndStartKMeans.class);
					action.setParamString(
							ReadCentroidsAndStartKMeans.S_INPUT_DIR, inDir);
					action.setParamString(
							ReadCentroidsAndStartKMeans.S_OUTPUT_DIR, outDir);
					actions.add(action);

					job.setActions(actions);
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
		} else {
			System.err.println("Command " + args[0]
					+ " NOT recognized. See usage.");
		}
	}
}
