package nl.vu.cs.ajira.examples.aurora.examples;

import nl.vu.cs.ajira.examples.aurora.api.ExecutionPath;
import nl.vu.cs.ajira.examples.aurora.data.Op;
import nl.vu.cs.ajira.examples.aurora.data.Pair;

public class MultiSplitExample extends AuroraExample {

  @Override
  protected void generatePath(ExecutionPath path) {
    path = path.addRandomTestGenerator(1, ExampleHelper.generateAttributeList("A", "B", "C", "D"), 1000);
    Pair<ExecutionPath> paths = path.split();
    ExecutionPath firstPath = paths.getFirstElement();
    ExecutionPath secondPath = paths.getSecondElement();
    firstPath = firstPath.addFilterOperator(ExampleHelper.generateFilter("B", Op.GT, 50));
    secondPath = secondPath.addFilterOperator(ExampleHelper.generateFilter("C", Op.GT, 50));

    paths = secondPath.split();
    ExecutionPath firstPath2 = paths.getFirstElement();
    ExecutionPath secondPath2 = paths.getSecondElement();
    secondPath2 = secondPath2.addFilterOperator(ExampleHelper.generateFilter("D", Op.GT, 50));
    secondPath = firstPath2.join("A", 10, secondPath2, true);

    path = firstPath.join("A", 100, secondPath, true);
    path = path.addTuplePrinter();
  }

}
