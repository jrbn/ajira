package nl.vu.cs.ajira.examples.aurora.examples;

import nl.vu.cs.ajira.examples.aurora.api.ExecutionPath;
import nl.vu.cs.ajira.examples.aurora.data.Op;

public class FilterExample extends AuroraExample {

  @Override
  protected void generatePath(ExecutionPath path) {
    path = path.addRandomTestGenerator(1, ExampleHelper.generateAttributeList("A", "B", "C"), 10000);
    path = path.addFilterOperator(ExampleHelper.generateFilter("A", Op.LT, 20));
    path = path.addTuplePrinter();
  }

}
