package nl.vu.cs.ajira.examples.aurora.examples;

import nl.vu.cs.ajira.Ajira;
import nl.vu.cs.ajira.submissions.Job;

public class ExampleRunner {

  public static void main(String[] args) {
    ExampleRunner runner = new ExampleRunner();
    Ajira ajira = new Ajira();
    //ajira.getConfiguration().set(InputLayer.INPUT_LAYER_CLASS, NetworkInputLayer.class.getName());
    try {
      ajira.startup();
    } catch (Exception e) {
      e.printStackTrace();
    }
    runner.runAllExamples(ajira);
    ajira.shutdown();
  }

  public void runAllExamples(Ajira ajira) {
    runFilterExample(ajira);
    runMapExample(ajira);
    runAggregateExample(ajira);
    runSortExample(ajira);
    runSingleSequenceExample(ajira);
    runSplitExample(ajira);
    runMultiSplitExample(ajira);
    runJoinExample(ajira);
    //runNetworkExample(ajira);
  }

  public void runFilterExample(Ajira ajira) {
    runExample(ajira, new FilterExample());
  }

  public void runMapExample(Ajira ajira) {
    runExample(ajira, new MapExample());
  }

  public void runAggregateExample(Ajira ajira) {
    runExample(ajira, new AggregateExample());
  }

  public void runSortExample(Ajira ajira) {
    runExample(ajira, new SortExample());
  }

  public void runSingleSequenceExample(Ajira ajira) {
    runExample(ajira, new SingleSequenceExample());
  }

  public void runSplitExample(Ajira ajira) {
    runExample(ajira, new SplitExample());
  }

  public void runMultiSplitExample(Ajira ajira) {
    runExample(ajira, new MultiSplitExample());
  }

  public void runJoinExample(Ajira ajira) {
    runExample(ajira, new JoinExample());
  }

  public void runNetworkExample(Ajira ajira) {
    runExample(ajira, new NetworkExample());
  }

  private void runExample(Ajira ajira, AuroraExample example) {
    Job job = example.generateExample();
    ajira.waitForCompletion(job);
  }

}
