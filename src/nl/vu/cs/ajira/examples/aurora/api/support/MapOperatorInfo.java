package nl.vu.cs.ajira.examples.aurora.api.support;

public class MapOperatorInfo implements OperatorInfo {
  private final String[] attributesToPreserve;

  public MapOperatorInfo(String... attributesToPreserve) {
    super();
    this.attributesToPreserve = attributesToPreserve;
  }

  public String[] getAttributesToPreserve() {
    return attributesToPreserve;
  }

}
