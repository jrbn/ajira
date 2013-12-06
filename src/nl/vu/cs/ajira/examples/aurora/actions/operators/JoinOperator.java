package nl.vu.cs.ajira.examples.aurora.actions.operators;

import java.util.List;

import nl.vu.cs.ajira.actions.ActionConf;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionFactory;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.actions.ActionSequence;
import nl.vu.cs.ajira.data.types.TByteArray;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.examples.aurora.actions.operators.helpers.AuroraJoinHelper;
import nl.vu.cs.ajira.examples.aurora.actions.operators.helpers.JoinHelper;
import nl.vu.cs.ajira.examples.aurora.actions.operators.helpers.WindowJoinHelper;
import nl.vu.cs.ajira.examples.aurora.utils.TupleSerializer;
import nl.vu.cs.ajira.exceptions.ActionNotConfiguredException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JoinOperator extends AuroraOperator {
  public static final int S_ATTRIBUTE = 0;
  public static final int I_SIZE = 1;
  public static final int I_CHANNEL_ID_1 = 2;
  public static final int I_CHANNEL_ID_2 = 3;
  public static final int B_WINDOW_JOIN = 4;

  private JoinHelper helper;
  private final Logger logger = LoggerFactory.getLogger(JoinOperator.class);

  public static void addToChain(String attribute, int size, int channelId1, int channelId2, boolean winJoin, ActionSequence actions) throws ActionNotConfiguredException {
    ActionConf c = ActionFactory.getActionConf(JoinOperator.class);
    c.setParamString(S_ATTRIBUTE, attribute);
    c.setParamInt(I_SIZE, size);
    c.setParamInt(I_CHANNEL_ID_1, channelId1);
    c.setParamInt(I_CHANNEL_ID_2, channelId2);
    c.setParamBoolean(B_WINDOW_JOIN, winJoin);
    actions.add(c);
  }

  @Override
  public void registerActionParameters(ActionConf conf) {
    conf.registerParameter(S_ATTRIBUTE, "attribute name", null, true);
    conf.registerParameter(I_SIZE, "size", 1, true);
    conf.registerParameter(I_CHANNEL_ID_1, "channel id 1", 0, true);
    conf.registerParameter(I_CHANNEL_ID_2, "channel id 2", 1, true);
    conf.registerParameter(B_WINDOW_JOIN, "window join", true, true);
  }

  @Override
  public void startProcess(ActionContext context) throws Exception {
    logger.debug("Starting JoinOperator");
    String attributeName = getParamString(S_ATTRIBUTE);
    int size = getParamInt(I_SIZE);
    int channelId1 = getParamInt(I_CHANNEL_ID_1);
    int channelId2 = getParamInt(I_CHANNEL_ID_2);
    boolean winJoin = getParamBoolean(B_WINDOW_JOIN);
    if (winJoin) {
      helper = new WindowJoinHelper(attributeName, size, channelId1, channelId2);
    } else {
      helper = new AuroraJoinHelper(attributeName, size, channelId1, channelId2);
    }
  }

  @Override
  public void process(Tuple tuple, ActionContext context, ActionOutput actionOutput) throws Exception {
    Tuple deserializedTuple = TupleSerializer.getTuple((TByteArray) tuple.get(0));
    List<Tuple> resultTuples = helper.push(deserializedTuple);
    if (resultTuples == null) {
      return;
    }
    for (Tuple resultTuple : resultTuples) {
      actionOutput.output(resultTuple);
    }
  }

}