package nl.vu.cs.ajira.examples.aurora.examples;

import nl.vu.cs.ajira.examples.aurora.api.ExecutionPath;
import nl.vu.cs.ajira.examples.aurora.api.JobGenerator;
import nl.vu.cs.ajira.submissions.Job;

public abstract class AuroraExample {

  public final Job generateExample() {
    ExecutionPath path = ExecutionPath.getExecutionPath();
    generatePath(path);
    return JobGenerator.generateJobFrom(path);
  }

  protected abstract void generatePath(ExecutionPath path);

}
