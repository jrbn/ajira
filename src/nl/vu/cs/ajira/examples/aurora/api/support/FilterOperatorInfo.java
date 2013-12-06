package nl.vu.cs.ajira.examples.aurora.api.support;

import nl.vu.cs.ajira.examples.aurora.data.Filter;

public class FilterOperatorInfo implements OperatorInfo {
  private final Filter filter;

  public FilterOperatorInfo(Filter filter) {
    this.filter = filter;
  }

  public Filter gerFilter() {
    return filter;
  }
}
