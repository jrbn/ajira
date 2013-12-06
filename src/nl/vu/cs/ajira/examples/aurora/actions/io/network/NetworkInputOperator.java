package nl.vu.cs.ajira.examples.aurora.actions.io.network;

import nl.vu.cs.ajira.actions.Action;
import nl.vu.cs.ajira.actions.ActionConf;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.actions.ActionController;
import nl.vu.cs.ajira.actions.ActionFactory;
import nl.vu.cs.ajira.actions.ActionOutput;
import nl.vu.cs.ajira.actions.ActionSequence;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.datalayer.InputQuery;
import nl.vu.cs.ajira.exceptions.ActionNotConfiguredException;

public class NetworkInputOperator extends Action {
  public static void addToChain(ActionSequence actions) throws ActionNotConfiguredException {
    ActionConf c = ActionFactory.getActionConf(NetworkInputOperator.class);
    actions.add(c);
  }

  @Override
  public void registerActionParameters(ActionConf conf) {
    conf.registerCustomConfigurator(new ParametersProcessor());
  }

  @Override
  public void process(Tuple tuple, ActionContext context, ActionOutput output) throws Exception {
    output.output(tuple);
  }

  private static class ParametersProcessor extends ActionConf.Configurator {
    @Override
    public void setupAction(InputQuery query, Object[] params, ActionController controller, ActionContext context) throws Exception {
      query.setInputLayer(NetworkInputLayer.class);
      controller.doNotAddCurrentAction();
    }
  }

}
