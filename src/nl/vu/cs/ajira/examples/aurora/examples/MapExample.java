package nl.vu.cs.ajira.examples.aurora.examples;

import nl.vu.cs.ajira.examples.aurora.api.ExecutionPath;

public class MapExample extends AuroraExample {

  @Override
  protected void generatePath(ExecutionPath path) {
    path = path.addRandomTestGenerator(1, ExampleHelper.generateAttributeList("A", "B", "C"), 10000);
    path = path.addMapOperator("A", "B");
    path = path.addTuplePrinter();
  }

}
