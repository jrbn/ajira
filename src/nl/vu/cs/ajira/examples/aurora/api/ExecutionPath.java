package nl.vu.cs.ajira.examples.aurora.api;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
import nl.vu.cs.ajira.examples.aurora.data.AggregationFunction;
import nl.vu.cs.ajira.examples.aurora.data.Filter;
import nl.vu.cs.ajira.examples.aurora.data.Ordering;
import nl.vu.cs.ajira.examples.aurora.data.Pair;

public abstract class ExecutionPath implements Iterable<OperatorInfo> {
  private static int lastId = 0;
  protected final int id;
  private final List<OperatorInfo> opInfo;
  private final Map<OperatorInfo, ExecutionPath> joiningPaths;
  private final Map<OperatorInfo, ExecutionPath> splitPaths;
  private final Map<ExecutionPath, Integer> splitPathStartPoints;

  protected ExecutionPath() {
    id = lastId++;
    opInfo = new ArrayList<OperatorInfo>();
    joiningPaths = new HashMap<OperatorInfo, ExecutionPath>();
    splitPaths = new HashMap<OperatorInfo, ExecutionPath>();
    splitPathStartPoints = new HashMap<ExecutionPath, Integer>();
  }

  public static ExecutionPath getExecutionPath() {
    if (lastId == 0) {
      return new RootExecutionPath();
    } else {
      return new BranchExecutionPath();
    }
  }

  public ExecutionPath addRandomTestGenerator(int id, int numThreads, List<String> attributes, int numTuples, int seed) {
    opInfo.add(new RandomTupleGeneratorOperatorInfo(id, numThreads, attributes, numTuples, seed));
    return this;
  }

  public ExecutionPath addRandomTestGenerator(int numThreads, List<String> attributes, int numTuples) {
    opInfo.add(new RandomTupleGeneratorOperatorInfo(0, numThreads, attributes, numTuples));
    return this;
  }

  public ExecutionPath addRandomTestReader(int id) {
    opInfo.add(new RandomTupleReaderOperatorInfo(id));
    return this;
  }

  public ExecutionPath addNetworkInputOperatorInfo() {
    opInfo.add(new NetworkInputOperatorInfo());
    return this;
  }

  public ExecutionPath addTuplePrinter() {
    opInfo.add(new OutputOperatorInfo());
    return this;
  }

  public ExecutionPath addAggregateOperator(String attributeName, int size, int advance, AggregationFunction function, String groupBy, Set<String> attributesToPreserve) {
    opInfo.add(new AggregateOperatorInfo(attributeName, size, advance, function, groupBy, attributesToPreserve));
    return this;
  }

  public ExecutionPath addAggregateOperator(String attributeName, int size, int advance, AggregationFunction function, Set<String> attributesToPreserve) {
    opInfo.add(new AggregateOperatorInfo(attributeName, size, advance, function, null, attributesToPreserve));
    return this;
  }

  public ExecutionPath addFilterOperator(Filter filter) {
    opInfo.add(new FilterOperatorInfo(filter));
    return this;
  }

  public ExecutionPath addMapOperator(String... attributesToPreserve) {
    opInfo.add(new MapOperatorInfo(attributesToPreserve));
    return this;
  }

  public ExecutionPath addSortOperator(String attributeName, int slack, Ordering ordering) {
    opInfo.add(new SortOperatorInfo(attributeName, slack, ordering));
    return this;
  }

  public Pair<ExecutionPath> split() {
    OperatorInfo info = new SplitOperatorInfo();
    opInfo.add(info);
    SplitExecutionPath splitPath = new SplitExecutionPath();
    splitPaths.put(info, splitPath);
    splitPathStartPoints.put(splitPath, opInfo.size());
    return new Pair<ExecutionPath>(this, splitPath);
  }

  public ExecutionPath join(String attribute, int size, ExecutionPath otherPath, boolean winJoin) {
    if (otherPath.id < id) {
      return otherPath.join(attribute, size, this, winJoin);
    }
    opInfo.add(new AddChannelOperatorInfo(0));
    otherPath.opInfo.add(new AddChannelOperatorInfo(1));
    JoinOperatorInfo info = new JoinOperatorInfo(attribute, size, 0, 1, winJoin);
    opInfo.add(info);
    if (otherPath instanceof BranchExecutionPath) {
      BranchExecutionPath branchExecutionPath = (BranchExecutionPath) otherPath;
      joiningPaths.put(info, branchExecutionPath);
    } else {
      SplitExecutionPath splitExecutionPath = (SplitExecutionPath) otherPath;
      int splitStart = splitPathStartPoints.get(splitExecutionPath);
      int splitEnd = opInfo.size() - 1;
      splitExecutionPath.setReconnectAfter(splitEnd - splitStart);
    }
    return this;
  }

  boolean hasBranchPath(OperatorInfo info) {
    return joiningPaths.containsKey(info);
  }

  ExecutionPath getBranchPath(OperatorInfo info) {
    assert (joiningPaths.containsKey(info));
    return joiningPaths.get(info);
  }

  SplitExecutionPath getSplitPath(OperatorInfo info) {
    assert (splitPaths.containsKey(info));
    ExecutionPath splitPath = splitPaths.get(info);
    assert (splitPath instanceof SplitExecutionPath);
    return (SplitExecutionPath) splitPath;
  }

  @Override
  public Iterator<OperatorInfo> iterator() {
    return opInfo.iterator();
  }

}
