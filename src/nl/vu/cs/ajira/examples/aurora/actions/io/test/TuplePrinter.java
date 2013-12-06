package nl.vu.cs.ajira.examples.aurora.actions.io.test;

import nl.vu.cs.ajira.actions.Action;
import nl.vu.cs.ajira.actions.ActionConf;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionFactory;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.actions.ActionSequence;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.exceptions.ActionNotConfiguredException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TuplePrinter extends Action {
  public static void addToChain(ActionSequence actions) throws ActionNotConfiguredException {
    ActionConf c = ActionFactory.getActionConf(TuplePrinter.class);
    actions.add(c);
  }

  private int numProcessed = 0;
  private final Logger logger = LoggerFactory.getLogger(TuplePrinter.class);

  @Override
  public void process(Tuple tuple, ActionContext context, ActionOutput actionOutput) throws Exception {
    numProcessed++;
  }

  @Override
  public void stopProcess(ActionContext context, ActionOutput actionOutput) throws Exception {
    logger.debug("Processed " + numProcessed + " tuples");
  }
}
