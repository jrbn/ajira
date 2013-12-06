package nl.vu.cs.ajira.examples.aurora.test;

import nl.vu.cs.ajira.Ajira;
import nl.vu.cs.ajira.actions.ActionConf;
import nl.vu.cs.ajira.actions.ActionFactory;
import nl.vu.cs.ajira.actions.ActionSequence;
import nl.vu.cs.ajira.actions.Branch;
import nl.vu.cs.ajira.actions.CollectToNode;
import nl.vu.cs.ajira.data.types.TInt;
import nl.vu.cs.ajira.datalayer.InputLayer;
import nl.vu.cs.ajira.submissions.Job;

public class CollectToNodeTest {

  public static void main(String args[]) {
    Ajira ajira = new Ajira();
    ajira.getConfiguration().set(InputLayer.INPUT_LAYER_CLASS, RandomInputLayer.class.getName());
    try {
      ajira.startup();
    } catch (Exception e1) {
      e1.printStackTrace();
    }
    Job job = new Job();
    try {
      job.setActions(generateSequence());
    } catch (Exception e) {
      e.printStackTrace();
    }
    double startTime = System.nanoTime();
    ajira.waitForCompletion(job);
    double endTime = System.nanoTime();
    System.out.println("Total time: " + (endTime - startTime) / 1000000.0 + " ms");
    ajira.shutdown();
  }

  private static final ActionSequence generateSequence() throws Exception {
    ActionSequence seq = new ActionSequence();
    RandomInputAction.addToChain(10, seq);
    EmptyActionBefore.addToChain(seq);

    ActionSequence seq2 = new ActionSequence();
    RandomInputAction.addToChain(20, seq2);
    EmptyActionBefore.addToChain(seq2);

    ActionConf a = ActionFactory.getActionConf(Branch.class);
    a.setParamWritable(Branch.W_BRANCH, seq2);
    seq.add(a);

    a = ActionFactory.getActionConf(CollectToNode.class);
    a.setParamStringArray(CollectToNode.SA_TUPLE_FIELDS, TInt.class.getName());
    seq.add(a);

    EmptyActionAfter.addToChain(seq);
    return seq;
  }

}
