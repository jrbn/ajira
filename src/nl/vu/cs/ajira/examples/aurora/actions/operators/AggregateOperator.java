package nl.vu.cs.ajira.examples.aurora.actions.operators;

import java.util.HashSet;
import java.util.Set;

import nl.vu.cs.ajira.actions.ActionConf;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionFactory;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.actions.ActionSequence;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.examples.aurora.actions.operators.helpers.AggregateHelper;
import nl.vu.cs.ajira.examples.aurora.data.AggregationFunction;
import nl.vu.cs.ajira.examples.aurora.data.StreamTuple;
import nl.vu.cs.ajira.exceptions.ActionNotConfiguredException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AggregateOperator extends AuroraOperator {
  public static final int S_ATTRIBUTE = 0;
  public static final int I_SIZE = 1;
  public static final int I_ADVANCE = 2;
  public static final int I_FUNCTION = 3;
  public static final int S_GROUP_BY = 4;
  public static final int SA_ATTRIBUTES_TO_PRESERVE = 5;

  private String attributeName;
  private int size;
  private int advance;
  private AggregationFunction function;
  private String groupBy;
  private Set<String> attributesToPreserve;

  private AggregateHelper helper;

  private final Logger logger = LoggerFactory.getLogger(AggregateOperator.class);

  public static void addToChain(String attribute, int size, int advance, AggregationFunction function, String groupBy, Set<String> attributesToPreserve, ActionSequence actions) throws ActionNotConfiguredException {
    ActionConf c = ActionFactory.getActionConf(AggregateOperator.class);
    c.setParamString(S_ATTRIBUTE, attribute);
    c.setParamInt(I_SIZE, size);
    c.setParamInt(I_ADVANCE, advance);
    c.setParamInt(I_FUNCTION, function.ordinal());
    c.setParamString(S_GROUP_BY, groupBy);
    c.setParamStringArray(SA_ATTRIBUTES_TO_PRESERVE, attributesToPreserve.toArray(new String[attributesToPreserve.size()]));
    actions.add(c);
  }

  public static void addToChain(String attribute, int size, int advance, AggregationFunction function, Set<String> attributesToPreserve, ActionSequence actions) throws ActionNotConfiguredException {
    ActionConf c = ActionFactory.getActionConf(AggregateOperator.class);
    c.setParamString(S_ATTRIBUTE, attribute);
    c.setParamInt(I_SIZE, size);
    c.setParamInt(I_ADVANCE, advance);
    c.setParamInt(I_FUNCTION, function.ordinal());
    c.setParamStringArray(SA_ATTRIBUTES_TO_PRESERVE, attributesToPreserve.toArray(new String[attributesToPreserve.size()]));
    actions.add(c);
  }

  @Override
  public void registerActionParameters(ActionConf conf) {
    conf.registerParameter(S_ATTRIBUTE, "attribute name", null, true);
    conf.registerParameter(I_SIZE, "size", 1, true);
    conf.registerParameter(I_ADVANCE, "advance", 1, true);
    conf.registerParameter(I_FUNCTION, "aggregation function", 0, true);
    conf.registerParameter(S_GROUP_BY, "group-by attribute", null, false);
    conf.registerParameter(SA_ATTRIBUTES_TO_PRESERVE, "attributes to preserve", null, true);
  }

  @Override
  public void startProcess(ActionContext context) throws Exception {
    logger.debug("Starting Aggregate Operator");
    attributeName = getParamString(S_ATTRIBUTE);
    size = getParamInt(I_SIZE);
    advance = getParamInt(I_ADVANCE);
    function = AggregationFunction.values()[getParamInt(I_FUNCTION)];
    groupBy = getParamString(S_GROUP_BY);
    attributesToPreserve = new HashSet<String>();
    String[] attributesToPreserveArray = getParamStringArray(SA_ATTRIBUTES_TO_PRESERVE);
    for (int i = 0; i < attributesToPreserveArray.length; i++) {
      attributesToPreserve.add(attributesToPreserveArray[i]);
    }
  }

  @Override
  public void process(Tuple tuple, ActionContext context, ActionOutput actionOutput) throws Exception {
    StreamTuple streamTuple = new StreamTuple(tuple);
    if (helper == null) {
      helper = new AggregateHelper(streamTuple, attributeName, function, size, advance, attributesToPreserve, groupBy);
    }
    Tuple outputTuple = helper.push(streamTuple);
    if (outputTuple != null) {
      actionOutput.output(outputTuple);
    }
  }
}
