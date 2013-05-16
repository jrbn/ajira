package nl.vu.cs.ajira.test;

import nl.vu.cs.ajira.Ajira;
import nl.vu.cs.ajira.actions.Action;
import nl.vu.cs.ajira.actions.ActionConf;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionFactory;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.actions.ActionSequence;
import nl.vu.cs.ajira.actions.Branch;
import nl.vu.cs.ajira.actions.PartitionToNodes;
import nl.vu.cs.ajira.actions.ReadFromFiles;
import nl.vu.cs.ajira.actions.Split;
import nl.vu.cs.ajira.actions.WriteToFiles;
import nl.vu.cs.ajira.data.types.TString;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.exceptions.ActionNotConfiguredException;
import nl.vu.cs.ajira.submissions.Job;
import nl.vu.cs.ajira.submissions.Submission;
import nl.vu.cs.ajira.utils.Consts;

public class TestTermination {

	public static class A extends Action {

		@Override
		public void process(Tuple tuple, ActionContext context,
				ActionOutput actionOutput) throws Exception {
			actionOutput.output(tuple);
		}
	}

	public static class B extends Action {

		@Override
		public void process(Tuple tuple, ActionContext context,
				ActionOutput actionOutput) throws Exception {
			actionOutput.output(tuple);
		}
	}

	public static class C extends Action {

		@Override
		public void process(Tuple tuple, ActionContext context,
				ActionOutput actionOutput) throws Exception {
			actionOutput.output(tuple);
		}
	}

	public static class D extends Action {

		@Override
		public void process(Tuple tuple, ActionContext context,
				ActionOutput actionOutput) throws Exception {
			actionOutput.output(tuple);
		}
	}

	public static class E extends Action {

		@Override
		public void process(Tuple tuple, ActionContext context,
				ActionOutput actionOutput) throws Exception {
		}
	}

	public static class F extends Action {

		@Override
		public void process(Tuple tuple, ActionContext context,
				ActionOutput actionOutput) throws Exception {
		}
	}

	public static class G extends Action {

		@Override
		public void process(Tuple tuple, ActionContext context,
				ActionOutput actionOutput) throws Exception {
		}
	}

	public static Job createSimpleJob(String inDir, String outDir)
			throws ActionNotConfiguredException {
		Job job = new Job();
		ActionSequence actions = new ActionSequence();

		ActionConf c = ActionFactory.getActionConf(ReadFromFiles.class);
		c.setParamString(ReadFromFiles.S_PATH, inDir);
		actions.add(c);

		// A
		actions.add(ActionFactory.getActionConf(A.class));

		// Partition
		c = ActionFactory.getActionConf(PartitionToNodes.class);
		c.setParamStringArray(PartitionToNodes.SA_TUPLE_FIELDS,
				TString.class.getName());
		c.setParamInt(PartitionToNodes.I_NPARTITIONS_PER_NODE, 1);
		actions.add(c);

		// C
		actions.add(ActionFactory.getActionConf(C.class));

		// Write the results on files
		c = ActionFactory.getActionConf(WriteToFiles.class);
		c.setParamString(WriteToFiles.S_PATH, outDir);
		actions.add(c);

		job.setActions(actions);
		return job;
	}

	public static Job createBranchJob(String inDir, String outDir)
			throws ActionNotConfiguredException {
		Job job = new Job();
		ActionSequence actions = new ActionSequence();

		ActionConf c = ActionFactory.getActionConf(ReadFromFiles.class);
		c.setParamString(ReadFromFiles.S_PATH, inDir);
		actions.add(c);

		// A
		actions.add(ActionFactory.getActionConf(A.class));

		// Partition
		c = ActionFactory.getActionConf(PartitionToNodes.class);
		c.setParamStringArray(PartitionToNodes.SA_TUPLE_FIELDS,
				TString.class.getName());
		c.setParamInt(PartitionToNodes.I_NPARTITIONS_PER_NODE, 2);
		actions.add(c);

		// C
		actions.add(ActionFactory.getActionConf(C.class));

		// Partition
		c = ActionFactory.getActionConf(PartitionToNodes.class);
		c.setParamStringArray(PartitionToNodes.SA_TUPLE_FIELDS,
				TString.class.getName());
		c.setParamInt(PartitionToNodes.I_NPARTITIONS_PER_NODE, 2);
		actions.add(c);

		// D
		actions.add(ActionFactory.getActionConf(D.class));

		// Write the results on files
		c = ActionFactory.getActionConf(WriteToFiles.class);
		c.setParamString(WriteToFiles.S_PATH, outDir);
		actions.add(c);

		job.setActions(actions);
		return job;
	}

	public static Job createBranchSplitJob(String inDir, String outDir)
			throws ActionNotConfiguredException {
		Job job = new Job();
		ActionSequence actions = new ActionSequence();

		ActionConf c = ActionFactory.getActionConf(ReadFromFiles.class);
		c.setParamString(ReadFromFiles.S_PATH, inDir);
		actions.add(c);

		// A
		actions.add(ActionFactory.getActionConf(A.class));

		// Read the input files
		ActionSequence branchActions = new ActionSequence();
		c = ActionFactory.getActionConf(ReadFromFiles.class);
		c.setParamString(ReadFromFiles.S_PATH, inDir);
		branchActions.add(c);

		// Split
		c = ActionFactory.getActionConf(Split.class);
		ActionSequence l = new ActionSequence();
		l.add(ActionFactory.getActionConf(E.class));
		l.add(ActionFactory.getActionConf(F.class));
		c.setParamWritable(Split.W_SPLIT, l);
		c.setParamInt(Split.I_RECONNECT_AFTER_ACTIONS, 3);
		branchActions.add(c);

		// Branch
		c = ActionFactory.getActionConf(Branch.class);
		c.setParamWritable(Branch.W_BRANCH, branchActions);
		actions.add(c);

		// B
		actions.add(ActionFactory.getActionConf(B.class));

		// Partition
		c = ActionFactory.getActionConf(PartitionToNodes.class);
		c.setParamStringArray(PartitionToNodes.SA_TUPLE_FIELDS,
				TString.class.getName());
		c.setParamInt(PartitionToNodes.I_NPARTITIONS_PER_NODE, 2);
		actions.add(c);

		// C
		actions.add(ActionFactory.getActionConf(C.class));

		// Partition
		c = ActionFactory.getActionConf(PartitionToNodes.class);
		c.setParamStringArray(PartitionToNodes.SA_TUPLE_FIELDS,
				TString.class.getName());
		c.setParamInt(PartitionToNodes.I_NPARTITIONS_PER_NODE, 4);
		actions.add(c);

		// D
		actions.add(ActionFactory.getActionConf(D.class));

		// Write the results on files
		c = ActionFactory.getActionConf(WriteToFiles.class);
		c.setParamString(WriteToFiles.S_PATH, outDir);
		actions.add(c);

		job.setActions(actions);
		return job;
	}

	public static void main(String[] args) {

		// Start up the cluster
		Ajira ajira = new Ajira();
		ajira.getConfiguration().setInt(Consts.N_PROC_THREADS, 8);
		ajira.startup();

		// With this command we ensure that we submit the job only once
		if (ajira.amItheServer()) {

			try {
				Job job = createSimpleJob(args[0], args[1]);
				Submission sub = ajira.waitForCompletion(job);
				sub.printStatistics();

				Job job1 = createSimpleJob(args[0], args[1]);
				Submission sub1 = ajira.waitForCompletion(job1);
				sub1.printStatistics();

			} catch (Exception e) {
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
