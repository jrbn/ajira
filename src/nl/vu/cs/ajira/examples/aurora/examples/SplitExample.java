package nl.vu.cs.ajira.examples.aurora.examples;

import nl.vu.cs.ajira.examples.aurora.api.ExecutionPath;
import nl.vu.cs.ajira.examples.aurora.data.Op;
import nl.vu.cs.ajira.examples.aurora.data.Pair;

public class SplitExample extends AuroraExample {

  @Override
  protected void generatePath(ExecutionPath path) {
    path = path.addRandomTestGenerator(1, ExampleHelper.generateAttributeList("A", "B", "C", "D"), 1000);
    Pair<ExecutionPath> paths = path.split();
    ExecutionPath firstPath = paths.getFirstElement();
    ExecutionPath secondPath = paths.getSecondElement();
    firstPath = firstPath.addFilterOperator(ExampleHelper.generateFilter("B", Op.GT, 50));
    secondPath = secondPath.addFilterOperator(ExampleHelper.generateFilter("C", Op.GT, 50));
    path = firstPath.join("A", 100, secondPath, true);
    path = path.addTuplePrinter();
  }

}
