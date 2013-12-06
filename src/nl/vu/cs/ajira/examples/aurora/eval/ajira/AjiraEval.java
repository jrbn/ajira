package nl.vu.cs.ajira.examples.aurora.eval.ajira;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import nl.vu.cs.ajira.Ajira;
import nl.vu.cs.ajira.actions.ActionSequence;
import nl.vu.cs.ajira.datalayer.InputLayer;
import nl.vu.cs.ajira.examples.aurora.actions.io.test.RandomGeneratorInputLayer;
import nl.vu.cs.ajira.examples.aurora.api.ExecutionPath;
import nl.vu.cs.ajira.examples.aurora.api.JobGenerator;
import nl.vu.cs.ajira.examples.aurora.data.AggregationFunction;
import nl.vu.cs.ajira.examples.aurora.data.Op;
import nl.vu.cs.ajira.examples.aurora.data.Ordering;
import nl.vu.cs.ajira.exceptions.ActionNotConfiguredException;
import nl.vu.cs.ajira.submissions.Job;
import nl.vu.cs.ajira.utils.Consts;

public class AjiraEval {
  private static final String filePath = "Results/";
  private static final boolean printTuples = true;

  private static final int numEvents = 100000;
  private static final int numSeeds = 10;
  private static final int numThreads = 8;

  private static final boolean runSingleThreadExamples = false;
  private static final boolean runMultiThreadExamples = true;

  private static final boolean runFilter = false;
  private static final boolean runMap = false;
  private static final boolean runAggregate = false;
  private static final boolean runOrder = false;
  private static final boolean runJoin = false;
  private static final boolean runMapMultiFilters = false;
  private static final boolean runFilterChain = false;

  public static void main(String[] args) {
    AjiraEval runner = new AjiraEval();
    Ajira ajira = new Ajira();
    ajira.getConfiguration().set(InputLayer.INPUT_LAYER_CLASS, RandomGeneratorInputLayer.class.getName());
    ajira.getConfiguration().setInt(Consts.N_PROC_THREADS, numThreads);
    try {
      ajira.startup();
    } catch (Exception e) {
      e.printStackTrace();
    }
    if (runSingleThreadExamples) runner.runSingleThreadExamples(ajira);
    if (runMultiThreadExamples) runner.runMultiThreadExamples(ajira);
    ajira.shutdown();
  }

  private final void runSingleThreadExamples(Ajira ajira) {
    for (int seed = 0; seed <= numSeeds; seed++) {
      generateInput(ajira, seed, 1);
    }
    if (runFilter) {
      for (int seed = 0; seed <= numSeeds; seed++) {
        Job job = generateFilterExample(seed);
        runExample("AjiraFilter", ajira, job, seed, true);
      }
    }
    if (runMap) {
      for (int seed = 0; seed <= numSeeds; seed++) {
        Job job = generateMapExample(seed);
        runExample("AjiraMap", ajira, job, seed, true);
      }
    }
    if (runAggregate) {
      for (int seed = 0; seed <= numSeeds; seed++) {
        Job job = generateAggregateExample(seed);
        runExample("AjiraAggregate", ajira, job, seed, true);
      }
    }
    if (runOrder) {
      for (int seed = 0; seed <= numSeeds; seed++) {
        Job job = generateOrderExample(seed);
        runExample("AjiraOrder", ajira, job, seed, true);
      }
    }
    if (runJoin) {
      int maxWin = 100;
      for (int seed = 0; seed <= numSeeds; seed++) {
        for (int win = 5; win <= maxWin; win += 5) {
          Job job = generateJoinExample(seed, win);
          runExample("AjiraJoin", ajira, job, seed, win == maxWin);
        }
      }
    }
    if (runMapMultiFilters) {
      for (int seed = 0; seed <= numSeeds; seed++) {
        Job job = generateMapMultiFiltersExample();
        runExample("AjiraMapMultiFilters", ajira, job, seed, true);
      }
    }
    if (runFilterChain) {
      int maxLength = 100;
      for (int seed = 0; seed <= numSeeds; seed++) {
        for (int length = 5; length <= maxLength; length += 5) {
          Job job = generateFilterChainExample(length);
          runExample("AjiraFilterChain", ajira, job, seed, length == maxLength);
        }
      }
    }

  }

  private final void runMultiThreadExamples(Ajira ajira) {
    int maxNumThreads = 8;
    int maxLength = 100;
    for (int numThreads = 1; numThreads <= maxNumThreads; numThreads++) {
      for (int seed = 0; seed <= numSeeds; seed++) {
        generateInput(ajira, seed, numThreads);
      }
      for (int seed = 0; seed <= numSeeds; seed++) {
        for (int length = 5; length <= maxLength; length += 5) {
          Job job = generateFilterMultiThreadExample(numThreads, length);
          runExample("MultiThread_" + numThreads, ajira, job, seed, length == maxLength);
        }
      }
    }
  }

  private final void runExample(String name, Ajira ajira, Job job, int seed, boolean newLine) {
    long startTime = System.nanoTime();
    ajira.waitForCompletion(job);
    double totalLatency = ((System.nanoTime() - startTime)) / 1000000.0;
    // double perEventLatency = totalLatency / numEvents;
    if (seed > 0) writeToFile(name, totalLatency, newLine);
  }

  private final void generateInput(Ajira ajira, int seed, int numThreads) {
    ExecutionPath path = ExecutionPath.getExecutionPath();
    path = path.addRandomTestGenerator(seed, numThreads, EvalHelper.generateAttributeList("A", "B", "C", "D"), numEvents, seed);
    Job job = JobGenerator.generateJobFrom(path);
    ajira.waitForCompletion(job);

    if (runJoin) {
      path = ExecutionPath.getExecutionPath();
      path = path.addRandomTestGenerator(100 + seed, numThreads, EvalHelper.generateAttributeList("A", "B", "C"), numEvents, 100 + seed);
      job = JobGenerator.generateJobFrom(path);
      ajira.waitForCompletion(job);

      path = ExecutionPath.getExecutionPath();
      path = path.addRandomTestGenerator(200 + seed, numThreads, EvalHelper.generateAttributeList("A", "D", "E"), numEvents, 200 + seed);
      job = JobGenerator.generateJobFrom(path);
      ajira.waitForCompletion(job);
    }
  }

  private final Job generateFilterExample(int seed) {
    ExecutionPath path = ExecutionPath.getExecutionPath();
    path = path.addRandomTestReader(seed);
    path = path.addFilterOperator(EvalHelper.generateFilter("A", Op.GT, 50));
    if (printTuples) path = path.addTuplePrinter();
    return JobGenerator.generateJobFrom(path);
  }

  private final Job generateMapExample(int seed) {
    ExecutionPath path = ExecutionPath.getExecutionPath();
    path = path.addRandomTestReader(seed);
    path = path.addMapOperator("A", "B", "C");
    if (printTuples) path = path.addTuplePrinter();
    return JobGenerator.generateJobFrom(path);
  }

  private final Job generateAggregateExample(int seed) {
    ExecutionPath path = ExecutionPath.getExecutionPath();
    path = path.addRandomTestReader(seed);
    path = path.addAggregateOperator("A", 100, 1, AggregationFunction.AVG, EvalHelper.generateAttributeSet("A", "B", "C", "D"));
    if (printTuples) path = path.addTuplePrinter();
    return JobGenerator.generateJobFrom(path);
  }

  private final Job generateOrderExample(int seed) {
    ExecutionPath path = ExecutionPath.getExecutionPath();
    path = path.addRandomTestReader(seed);
    path = path.addSortOperator("A", 100, Ordering.ASCENDING);
    if (printTuples) path = path.addTuplePrinter();
    return JobGenerator.generateJobFrom(path);
  }

  private final Job generateJoinExample(int seed, int win) {
    ExecutionPath path = ExecutionPath.getExecutionPath();
    path = path.addRandomTestReader(100 + seed);

    ExecutionPath otherPath = ExecutionPath.getExecutionPath();
    otherPath = otherPath.addRandomTestReader(200 + seed);

    path = path.join("A", win, otherPath, true);
    if (printTuples) path = path.addTuplePrinter();
    return JobGenerator.generateJobFrom(path);
  }

  private final Job generateMapMultiFiltersExample() {
    ExecutionPath path = ExecutionPath.getExecutionPath();
    path = path.addRandomTestReader(0);
    path = path.addFilterOperator(EvalHelper.generateFilter("A", Op.GT, 50));
    path = path.addFilterOperator(EvalHelper.generateFilter("B", Op.GT, 0));
    path = path.addFilterOperator(EvalHelper.generateFilter("C", Op.LT, 100));
    path = path.addMapOperator("A", "B", "C");
    if (printTuples) path = path.addTuplePrinter();
    return JobGenerator.generateJobFrom(path);
  }

  private final Job generateFilterChainExample(int length) {
    ExecutionPath path = ExecutionPath.getExecutionPath();
    path = path.addRandomTestReader(0);
    for (int i = 0; i < length; i++) {
      path = path.addFilterOperator(EvalHelper.generateFilter("A", Op.GT, 0));
    }
    if (printTuples) path = path.addTuplePrinter();
    return JobGenerator.generateJobFrom(path);
  }

  private final Job generateFilterMultiThreadExample(int numThreads, int numActions) {
    Job job = new Job();
    ActionSequence seq = new ActionSequence();
    try {
      MultiThreadingController.addToChain(numThreads, numActions, seq);
    } catch (ActionNotConfiguredException e) {
      e.printStackTrace();
    }
    job.setActions(seq);
    return job;
  }

  private final void writeToFile(String fileName, double value, boolean printNewLine) {
    try {
      FileOutputStream os = new FileOutputStream(new File(filePath + fileName + ".txt"), true);
      String line = String.valueOf(value);
      if (printNewLine) {
        line = line + "\n";
      } else {
        line = line + "\t";
      }
      os.write(line.getBytes());
      os.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

}
