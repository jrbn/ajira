package nl.vu.cs.ajira.examples.aurora.api.support;

import java.util.List;

public class RandomTupleGeneratorOperatorInfo implements OperatorInfo {
  private final int id;
  private final int numThreads;
  private final List<String> attributes;
  private final int numTuples;
  private final int seed;

  public RandomTupleGeneratorOperatorInfo(int id, int numThreads, List<String> attributes, int numTuples, int seed) {
    super();
    this.id = id;
    this.numThreads = numThreads;
    this.attributes = attributes;
    this.numTuples = numTuples;
    this.seed = seed;
  }

  public RandomTupleGeneratorOperatorInfo(int id, int numThreads, List<String> attributes, int numTuples) {
    this(id, numThreads, attributes, numTuples, 0);
  }

  public List<String> getAttributes() {
    return attributes;
  }

  public int getNumTuples() {
    return numTuples;
  }

  public int getSeed() {
    return seed;
  }

  public int getId() {
    return id;
  }

  public int getNumThreads() {
    return numThreads;
  }

}
