package nl.vu.cs.ajira.examples.aurora.examples;

import nl.vu.cs.ajira.examples.aurora.api.ExecutionPath;

public class JoinExample extends AuroraExample {

  @Override
  protected void generatePath(ExecutionPath path) {
    path = path.addRandomTestGenerator(1, ExampleHelper.generateAttributeList("A", "B"), 1000);
    ExecutionPath secondPath = ExecutionPath.getExecutionPath();
    secondPath = secondPath.addRandomTestGenerator(1, ExampleHelper.generateAttributeList("A", "C"), 1000);
    path = path.join("A", 50, secondPath, true);
    path = path.addTuplePrinter();
  }

}
