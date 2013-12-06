package nl.vu.cs.ajira.examples.aurora.actions.operators;

import nl.vu.cs.ajira.actions.ActionConf;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionFactory;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.actions.ActionSequence;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.examples.aurora.actions.operators.helpers.FilterHelper;
import nl.vu.cs.ajira.examples.aurora.data.Filter;
import nl.vu.cs.ajira.examples.aurora.data.StreamTuple;
import nl.vu.cs.ajira.exceptions.ActionNotConfiguredException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FilterOperator extends AuroraOperator {
  private static final int W_FILTER = 0;
  private final Filter filter = new Filter();
  private FilterHelper helper = null;

  private final Logger logger = LoggerFactory.getLogger(FilterOperator.class);

  public static void addToChain(Filter filter, ActionSequence actions) throws ActionNotConfiguredException {
    ActionConf c = ActionFactory.getActionConf(FilterOperator.class);
    c.setParamWritable(W_FILTER, filter);
    actions.add(c);
  }

  @Override
  public void registerActionParameters(ActionConf conf) {
    conf.registerParameter(W_FILTER, "filter", null, true);
  }

  @Override
  public void startProcess(ActionContext context) throws Exception {
    logger.debug("Starting Filter Operator");
    getParamWritable(filter, W_FILTER);
  }

  @Override
  public void process(Tuple tuple, ActionContext context, ActionOutput actionOutput) throws Exception {
    if (helper == null) {
      helper = new FilterHelper(filter, new StreamTuple(tuple));
    }
    if (helper.isSatisfiedBy(tuple)) {
      actionOutput.output(tuple);
    }
  }
}
