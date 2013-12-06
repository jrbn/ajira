package nl.vu.cs.ajira.examples.aurora.actions.operators;

import java.util.HashSet;
import java.util.Set;

import nl.vu.cs.ajira.actions.ActionConf;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionFactory;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.actions.ActionSequence;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.examples.aurora.data.Projector;
import nl.vu.cs.ajira.exceptions.ActionNotConfiguredException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MapOperator extends AuroraOperator {
  private static final int SA_ATTRIBUTES = 0;
  private Projector projector;

  private final Logger logger = LoggerFactory.getLogger(MapOperator.class);

  public static void addToChain(ActionSequence actions, String... attributes) throws ActionNotConfiguredException {
    ActionConf c = ActionFactory.getActionConf(MapOperator.class);
    c.setParamStringArray(SA_ATTRIBUTES, attributes);
    actions.add(c);
  }

  @Override
  public void registerActionParameters(ActionConf conf) {
    conf.registerParameter(SA_ATTRIBUTES, "attributes to project", null, true);
  }

  @Override
  public void startProcess(ActionContext context) throws Exception {
    logger.debug("Starting Map Operator");
    String[] attributesToProject = getParamStringArray(SA_ATTRIBUTES);
    Set<String> attrSet = new HashSet<String>();
    for (String attr : attributesToProject) {
      attrSet.add(attr);
    }
    projector = new Projector(attrSet);
  }

  @Override
  public void process(Tuple tuple, ActionContext context, ActionOutput actionOutput) throws Exception {
    actionOutput.output(projector.project(tuple));
  }
}
