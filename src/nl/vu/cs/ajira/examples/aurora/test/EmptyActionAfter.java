package nl.vu.cs.ajira.examples.aurora.test;

import nl.vu.cs.ajira.actions.Action;
import nl.vu.cs.ajira.actions.ActionConf;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionFactory;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.actions.ActionSequence;
import nl.vu.cs.ajira.data.types.Tuple;

public class EmptyActionAfter extends Action {
  private int count = 0;

  public static final void addToChain(ActionSequence seq) throws Exception {
    ActionConf c = ActionFactory.getActionConf(EmptyActionAfter.class);
    seq.add(c);
  }

  @Override
  public void startProcess(ActionContext context) throws Exception {

  }

  @Override
  public void process(Tuple tuple, ActionContext context, ActionOutput actionOutput) throws Exception {
    count++;
  }

  @Override
  public void stopProcess(ActionContext context, ActionOutput actionOutput) throws Exception {
    System.out.println("EmptyActionAfter processed " + count + " tuples");
  }

}
