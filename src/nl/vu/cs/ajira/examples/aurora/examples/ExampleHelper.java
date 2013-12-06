package nl.vu.cs.ajira.examples.aurora.examples;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import nl.vu.cs.ajira.examples.aurora.data.Constraint;
import nl.vu.cs.ajira.examples.aurora.data.Filter;
import nl.vu.cs.ajira.examples.aurora.data.Op;

class ExampleHelper {

  static List<String> generateAttributeList(String... attributes) {
    List<String> results = new ArrayList<String>();
    for (String attribute : attributes) {
      results.add(attribute);
    }
    return results;
  }

  static Set<String> generateAttributeSet(String... attributes) {
    Set<String> results = new HashSet<String>();
    for (String attribute : attributes) {
      results.add(attribute);
    }
    return results;
  }

  static Filter generateFilter(String attribute, Op op, int val) {
    Constraint c = new Constraint(attribute, op, val);
    Set<Constraint> constraints = new HashSet<Constraint>();
    constraints.add(c);
    return new Filter(constraints);
  }
}
