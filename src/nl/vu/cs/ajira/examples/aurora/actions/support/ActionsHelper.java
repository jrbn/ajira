package nl.vu.cs.ajira.examples.aurora.actions.support;

import nl.vu.cs.ajira.actions.ActionConf;
import nl.vu.cs.ajira.actions.ActionFactory;
import nl.vu.cs.ajira.actions.ActionSequence;
import nl.vu.cs.ajira.actions.CollectToNode;
import nl.vu.cs.ajira.actions.QueryInputLayer;
import nl.vu.cs.ajira.actions.support.Query;
import nl.vu.cs.ajira.data.types.TByteArray;
import nl.vu.cs.ajira.datalayer.dummy.DummyLayer;
import nl.vu.cs.ajira.exceptions.ActionNotConfiguredException;

public class ActionsHelper {

  public static void collectToNode(ActionSequence actions) throws ActionNotConfiguredException {
    ActionConf a = ActionFactory.getActionConf(CollectToNode.class);
    a.setParamStringArray(CollectToNode.SA_TUPLE_FIELDS, TByteArray.class.getName());
    actions.add(a);
  }

  public static void readFakeTuple(ActionSequence actions) throws ActionNotConfiguredException {
    ActionConf a = ActionFactory.getActionConf(QueryInputLayer.class);
    a.setParamString(QueryInputLayer.S_INPUTLAYER, DummyLayer.class.getName());
    a.setParamWritable(QueryInputLayer.W_QUERY, new Query());
    actions.add(a);
  }
}
