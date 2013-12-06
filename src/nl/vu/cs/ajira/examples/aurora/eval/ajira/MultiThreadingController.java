package nl.vu.cs.ajira.examples.aurora.eval.ajira;

import nl.vu.cs.ajira.actions.Action;
import nl.vu.cs.ajira.actions.ActionConf;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionFactory;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.actions.ActionSequence;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.examples.aurora.actions.io.test.RandomTupleReaderAction;
import nl.vu.cs.ajira.examples.aurora.actions.io.test.TuplePrinter;
import nl.vu.cs.ajira.examples.aurora.actions.operators.FilterOperator;
import nl.vu.cs.ajira.examples.aurora.actions.support.ActionsHelper;
import nl.vu.cs.ajira.examples.aurora.data.Op;
import nl.vu.cs.ajira.exceptions.ActionNotConfiguredException;

public class MultiThreadingController extends Action {
  public static final int I_NUM_THREADS = 0;
  public static final int I_NUM_ACTIONS = 1;
  private int numThreads;
  private int numActions;

  public static void addToChain(int numThreads, int numActions, ActionSequence actions) throws ActionNotConfiguredException {
    ActionConf c = ActionFactory.getActionConf(MultiThreadingController.class);
    c.setParamInt(I_NUM_THREADS, numThreads);
    c.setParamInt(I_NUM_ACTIONS, numActions);
    ActionsHelper.readFakeTuple(actions);
    actions.add(c);
  }

  @Override
  public void registerActionParameters(ActionConf conf) {
    conf.registerParameter(I_NUM_THREADS, "number of threads", 1, true);
    conf.registerParameter(I_NUM_ACTIONS, "number of actions", 1, true);
  }

  @Override
  public void startProcess(ActionContext context) throws Exception {
    numThreads = getParamInt(I_NUM_THREADS);
    numActions = getParamInt(I_NUM_ACTIONS);
  }

  @Override
  public void process(Tuple tuple, ActionContext context, ActionOutput actionOutput) throws Exception {
    // Nothing to do
  }

  @Override
  public void stopProcess(ActionContext context, ActionOutput actionOutput) throws Exception {
    for (int i = 0; i < numThreads; i++) {
      actionOutput.branch(generateActionSequence(i, numActions));
    }
  }

  private ActionSequence generateActionSequence(int threadId, int numActions) throws ActionNotConfiguredException {
    ActionSequence seq = new ActionSequence();
    RandomTupleReaderAction.addToChain(0, threadId, seq);
    for (int i = 0; i < numActions; i++) {
      FilterOperator.addToChain(EvalHelper.generateFilter("A", Op.GT, 0), seq);
    }
    TuplePrinter.addToChain(seq);
    return seq;
  }
}
