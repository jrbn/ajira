package nl.vu.cs.ajira.examples.aurora.api.support;

public class RandomTupleReaderOperatorInfo implements OperatorInfo {
  private final int id;

  public RandomTupleReaderOperatorInfo(int id) {
    super();
    this.id = id;
  }

  public int getId() {
    return id;
  }

}
