package nl.vu.cs.ajira.examples.aurora.api;

import nl.vu.cs.ajira.actions.ActionConf;
import nl.vu.cs.ajira.actions.ActionFactory;
import nl.vu.cs.ajira.actions.ActionSequence;
import nl.vu.cs.ajira.actions.Branch;
import nl.vu.cs.ajira.actions.Split;
import nl.vu.cs.ajira.examples.aurora.actions.io.network.NetworkInputOperator;
import nl.vu.cs.ajira.examples.aurora.actions.io.test.RandomTupleGeneratorAction;
import nl.vu.cs.ajira.examples.aurora.actions.io.test.RandomTupleReaderAction;
import nl.vu.cs.ajira.examples.aurora.actions.io.test.TuplePrinter;
import nl.vu.cs.ajira.examples.aurora.actions.operators.AddChannelOperator;
import nl.vu.cs.ajira.examples.aurora.actions.operators.AggregateOperator;
import nl.vu.cs.ajira.examples.aurora.actions.operators.FilterOperator;
import nl.vu.cs.ajira.examples.aurora.actions.operators.JoinOperator;
import nl.vu.cs.ajira.examples.aurora.actions.operators.MapOperator;
import nl.vu.cs.ajira.examples.aurora.actions.operators.SortOperator;
import nl.vu.cs.ajira.examples.aurora.actions.support.ActionsHelper;
import nl.vu.cs.ajira.examples.aurora.actions.support.EmptyAction;
import nl.vu.cs.ajira.examples.aurora.api.support.AddChannelOperatorInfo;
import nl.vu.cs.ajira.examples.aurora.api.support.AggregateOperatorInfo;
import nl.vu.cs.ajira.examples.aurora.api.support.FilterOperatorInfo;
import nl.vu.cs.ajira.examples.aurora.api.support.JoinOperatorInfo;
import nl.vu.cs.ajira.examples.aurora.api.support.MapOperatorInfo;
import nl.vu.cs.ajira.examples.aurora.api.support.NetworkInputOperatorInfo;
import nl.vu.cs.ajira.examples.aurora.api.support.OperatorInfo;
import nl.vu.cs.ajira.examples.aurora.api.support.OutputOperatorInfo;
import nl.vu.cs.ajira.examples.aurora.api.support.RandomTupleGeneratorOperatorInfo;
import nl.vu.cs.ajira.examples.aurora.api.support.RandomTupleReaderOperatorInfo;
import nl.vu.cs.ajira.examples.aurora.api.support.SortOperatorInfo;
import nl.vu.cs.ajira.examples.aurora.api.support.SplitOperatorInfo;
import nl.vu.cs.ajira.submissions.Job;

public class JobGenerator {

  public static Job generateJobFrom(ExecutionPath path) {
    ActionSequence sequence = generateSequenceFrom(path);
    Job job = new Job();
    job.setActions(sequence);
    return job;
  }

  private static ActionSequence generateSequenceFrom(ExecutionPath path) {
    ActionSequence sequence = new ActionSequence();
    for (OperatorInfo info : path) {
      try {
        addOperator(sequence, info, path);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    return sequence;
  }

  private static void addOperator(ActionSequence sequence, OperatorInfo info, ExecutionPath path) throws Exception {
    if (info instanceof AddChannelOperatorInfo) {
      addOperator(sequence, (AddChannelOperatorInfo) info);
    } else if (info instanceof AggregateOperatorInfo) {
      addOperator(sequence, (AggregateOperatorInfo) info);
    } else if (info instanceof FilterOperatorInfo) {
      addOperator(sequence, (FilterOperatorInfo) info);
    } else if (info instanceof MapOperatorInfo) {
      addOperator(sequence, (MapOperatorInfo) info);
    } else if (info instanceof JoinOperatorInfo) {
      addBranchIfNeeded(sequence, info, path);
      addOperator(sequence, (JoinOperatorInfo) info);
    } else if (info instanceof SortOperatorInfo) {
      addOperator(sequence, (SortOperatorInfo) info);
    } else if (info instanceof SplitOperatorInfo) {
      addSplit(sequence, info, path);
    } else if (info instanceof RandomTupleGeneratorOperatorInfo) {
      addOperator(sequence, (RandomTupleGeneratorOperatorInfo) info);
    } else if (info instanceof NetworkInputOperatorInfo) {
      addOperator(sequence, (NetworkInputOperatorInfo) info);
    } else if (info instanceof OutputOperatorInfo) {
      addOperator(sequence, (OutputOperatorInfo) info);
    } else if (info instanceof RandomTupleReaderOperatorInfo) {
      addOperator(sequence, (RandomTupleReaderOperatorInfo) info);
    }
  }

  private static void addBranchIfNeeded(ActionSequence sequence, OperatorInfo info, ExecutionPath path) throws Exception {
    if (path.hasBranchPath(info)) {
      ExecutionPath branchPath = path.getBranchPath(info);
      ActionSequence branchSequence = generateSequenceFrom(branchPath);
      ActionConf actionConf = ActionFactory.getActionConf(Branch.class);
      actionConf.setParamWritable(Branch.W_BRANCH, branchSequence);
      sequence.add(actionConf);
    }
  }

  private static void addSplit(ActionSequence sequence, OperatorInfo info, ExecutionPath path) throws Exception {
    SplitExecutionPath splitPath = path.getSplitPath(info);
    ActionSequence splitSequence = generateSequenceFrom(splitPath);
    ActionConf actionConf = ActionFactory.getActionConf(Split.class);
    actionConf.setParamWritable(Split.W_SPLIT, splitSequence);
    actionConf.setParamInt(Split.I_RECONNECT_AFTER_ACTIONS, splitPath.getReconnectAfter());
    sequence.add(actionConf);
  }

  private static void addOperator(ActionSequence sequence, AddChannelOperatorInfo info) throws Exception {
    AddChannelOperator.addToChain(info.getChannelId(), sequence);
  }

  private static void addOperator(ActionSequence sequence, AggregateOperatorInfo info) throws Exception {
    AggregateOperator.addToChain(info.getAttributeName(), info.getSize(), info.getAdvance(), info.getFunction(), info.getAttributesToPreserve(), sequence);
  }

  private static void addOperator(ActionSequence sequence, FilterOperatorInfo info) throws Exception {
    FilterOperator.addToChain(info.gerFilter(), sequence);
  }

  private static void addOperator(ActionSequence sequence, JoinOperatorInfo info) throws Exception {
    ActionsHelper.collectToNode(sequence);
    JoinOperator.addToChain(info.getAttributeName(), info.getSize(), info.getChannelId1(), info.getChannelId2(), info.getWinJoin(), sequence);
  }

  private static void addOperator(ActionSequence sequence, MapOperatorInfo info) throws Exception {
    MapOperator.addToChain(sequence, info.getAttributesToPreserve());
  }

  private static void addOperator(ActionSequence sequence, SortOperatorInfo info) throws Exception {
    SortOperator.addToChain(info.getAttributeName(), info.getSlack(), info.getOrdering(), sequence);
  }

  private static void addOperator(ActionSequence sequence, RandomTupleGeneratorOperatorInfo info) throws Exception {
    RandomTupleGeneratorAction.addToChain(info.getId(), info.getNumThreads(), info.getAttributes(), info.getNumTuples(), info.getSeed(), sequence);
    EmptyAction.addToChain(sequence);
  }

  private static void addOperator(ActionSequence sequence, NetworkInputOperatorInfo info) throws Exception {
    NetworkInputOperator.addToChain(sequence);
  }

  private static void addOperator(ActionSequence sequence, OutputOperatorInfo info) throws Exception {
    TuplePrinter.addToChain(sequence);
  }

  private static void addOperator(ActionSequence sequence, RandomTupleReaderOperatorInfo info) throws Exception {
    RandomTupleReaderAction.addToChain(info.getId(), 0, sequence);
  }

}
