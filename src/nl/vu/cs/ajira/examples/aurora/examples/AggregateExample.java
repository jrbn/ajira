package nl.vu.cs.ajira.examples.aurora.examples;

import nl.vu.cs.ajira.examples.aurora.api.ExecutionPath;
import nl.vu.cs.ajira.examples.aurora.data.AggregationFunction;

public class AggregateExample extends AuroraExample {

  @Override
  protected void generatePath(ExecutionPath path) {
    path = path.addRandomTestGenerator(1, ExampleHelper.generateAttributeList("A", "B", "C"), 10000);
    path = path.addTuplePrinter();
    path = path.addAggregateOperator("A", 10, 10, AggregationFunction.SUM, ExampleHelper.generateAttributeSet("A", "B"));
    path = path.addTuplePrinter();
  }

}
