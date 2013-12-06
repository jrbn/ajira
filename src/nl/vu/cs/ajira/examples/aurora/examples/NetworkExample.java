package nl.vu.cs.ajira.examples.aurora.examples;

import nl.vu.cs.ajira.examples.aurora.api.ExecutionPath;
import nl.vu.cs.ajira.examples.aurora.data.Op;

public class NetworkExample extends AuroraExample {

  @Override
  protected void generatePath(ExecutionPath path) {
    path = path.addNetworkInputOperatorInfo();
    path = path.addFilterOperator(ExampleHelper.generateFilter("A", Op.GT, 50));
    path = path.addTuplePrinter();
  }

}
