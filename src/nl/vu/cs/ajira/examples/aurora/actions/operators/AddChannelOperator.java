package nl.vu.cs.ajira.examples.aurora.actions.operators;

import nl.vu.cs.ajira.actions.ActionConf;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionFactory;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.actions.ActionSequence;
import nl.vu.cs.ajira.data.types.TByteArray;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.data.types.TupleFactory;
import nl.vu.cs.ajira.examples.aurora.utils.TupleSerializer;
import nl.vu.cs.ajira.exceptions.ActionNotConfiguredException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AddChannelOperator extends AuroraOperator {
  public static final int I_CHANNEL_ID = 0;

  private int channelId;
  private final Tuple outputTuple = TupleFactory.newTuple();

  private static final Logger logger = LoggerFactory.getLogger(AddChannelOperator.class);

  public static void addToChain(int channel, ActionSequence actions) throws ActionNotConfiguredException {
    ActionConf c = ActionFactory.getActionConf(AddChannelOperator.class);
    c.setParamInt(I_CHANNEL_ID, channel);
    actions.add(c);
  }

  @Override
  public void registerActionParameters(ActionConf conf) {
    conf.registerParameter(I_CHANNEL_ID, "channel id", 0, true);
  }

  @Override
  public void startProcess(ActionContext context) throws Exception {
    channelId = getParamInt(I_CHANNEL_ID);
    logger.debug("Starting addChannel with id = " + channelId);
  }

  @Override
  public void process(Tuple tuple, ActionContext context, ActionOutput actionOutput) throws Exception {
    TByteArray byteArray = new TByteArray(TupleSerializer.encodeTuple(tuple, channelId));
    outputTuple.set(byteArray);
    actionOutput.output(outputTuple);
  }

}