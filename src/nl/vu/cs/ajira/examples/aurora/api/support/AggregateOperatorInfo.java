package nl.vu.cs.ajira.examples.aurora.api.support;

import java.util.Set;

import nl.vu.cs.ajira.examples.aurora.data.AggregationFunction;

public class AggregateOperatorInfo implements OperatorInfo {
  private final String attributeName;
  private final int size;
  private final int advance;
  private final AggregationFunction function;
  private final String groupBy;
  private final Set<String> attributesToPreserve;

  public AggregateOperatorInfo(String attributeName, int size, int advance, AggregationFunction function, String groupBy, Set<String> attributesToPreserve) {
    super();
    this.attributeName = attributeName;
    this.size = size;
    this.advance = advance;
    this.function = function;
    this.groupBy = groupBy;
    this.attributesToPreserve = attributesToPreserve;
  }

  public String getAttributeName() {
    return attributeName;
  }

  public int getSize() {
    return size;
  }

  public int getAdvance() {
    return advance;
  }

  public AggregationFunction getFunction() {
    return function;
  }

  public String getGroupBy() {
    return groupBy;
  }

  public Set<String> getAttributesToPreserve() {
    return attributesToPreserve;
  }

}
