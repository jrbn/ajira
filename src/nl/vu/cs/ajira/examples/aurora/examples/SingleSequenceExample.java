package nl.vu.cs.ajira.examples.aurora.examples;

import nl.vu.cs.ajira.examples.aurora.api.ExecutionPath;
import nl.vu.cs.ajira.examples.aurora.data.AggregationFunction;
import nl.vu.cs.ajira.examples.aurora.data.Op;
import nl.vu.cs.ajira.examples.aurora.data.Ordering;

public class SingleSequenceExample extends AuroraExample {

  @Override
  protected void generatePath(ExecutionPath path) {
    path = path.addRandomTestGenerator(1, ExampleHelper.generateAttributeList("A", "B", "C", "D"), 1000);
    path = path.addAggregateOperator("A", 10, 5, AggregationFunction.AVG, ExampleHelper.generateAttributeSet("A", "B", "C"));
    path = path.addFilterOperator(ExampleHelper.generateFilter("A", Op.GT, 20));
    path = path.addMapOperator("A", "B");
    path = path.addSortOperator("A", 10, Ordering.ASCENDING);
    path = path.addTuplePrinter();
  }

}
