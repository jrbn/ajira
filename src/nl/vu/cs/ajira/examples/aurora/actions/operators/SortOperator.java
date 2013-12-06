package nl.vu.cs.ajira.examples.aurora.actions.operators;

import nl.vu.cs.ajira.actions.ActionConf;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionFactory;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.actions.ActionSequence;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.examples.aurora.actions.operators.helpers.SortHelper;
import nl.vu.cs.ajira.examples.aurora.data.Ordering;
import nl.vu.cs.ajira.exceptions.ActionNotConfiguredException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SortOperator extends AuroraOperator {
  public static final int S_ATTRIBUTE = 0;
  public static final int I_SLACK = 1;
  public static final int B_ASCENDING = 2;

  private int slack;
  private String attributeName;
  private Ordering ordering;

  private SortHelper helper;

  private final Logger logger = LoggerFactory.getLogger(SortOperator.class);

  public static void addToChain(String attribute, int slack, Ordering ordering, ActionSequence actions) throws ActionNotConfiguredException {
    ActionConf c = ActionFactory.getActionConf(SortOperator.class);
    c.setParamString(S_ATTRIBUTE, attribute);
    c.setParamInt(I_SLACK, slack);
    c.setParamBoolean(B_ASCENDING, ordering == Ordering.ASCENDING);
    actions.add(c);
  }

  @Override
  public void registerActionParameters(ActionConf conf) {
    conf.registerParameter(S_ATTRIBUTE, "attribute name", null, true);
    conf.registerParameter(I_SLACK, "slack size", 1, true);
    conf.registerParameter(B_ASCENDING, "ordering", true, true);
  }

  @Override
  public void startProcess(ActionContext context) throws Exception {
    logger.debug("Starting Sort Operator");
    attributeName = getParamString(S_ATTRIBUTE);
    slack = getParamInt(I_SLACK);
    ordering = getParamBoolean(B_ASCENDING) ? Ordering.ASCENDING : Ordering.DESCENDING;
  }

  @Override
  public void process(Tuple tuple, ActionContext context, ActionOutput actionOutput) throws Exception {
    if (helper == null) {
      helper = new SortHelper(tuple, attributeName, slack, ordering);
    }
    Tuple outputTuple = helper.push(tuple);
    if (outputTuple != null) {
      actionOutput.output(outputTuple);
    }
  }
}
