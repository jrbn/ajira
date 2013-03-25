package nl.vu.cs.ajira.actions;

import java.util.List;

import nl.vu.cs.ajira.data.types.SimpleData;
import nl.vu.cs.ajira.data.types.Tuple;

/**
 * This interface defines the methods that can be used to make actions produce
 * results. Ajira passess an <code>ActionOutput</code> object on to the
 * {@link Action#process(Tuple, ActionContext, ActionOutput)} and the
 * {@link Action#stopProcess(ActionContext, ActionOutput)} methods, allowing
 * actions to produce results.
 */
public interface ActionOutput {

	/**
	 * This method creates a {@link Tuple} from the specified parameter(s),
	 * and passes that on as result, usually to the next action in the chain.
	 * @param data the data to pass on.
	 * @throws Exception
	 */
	public void output(SimpleData... data) throws Exception;

	/**
	 * This method passes the specified tuple on as result, usually to the next action
	 * in the chain.
	 * @param data the data to pass on.
	 * @throws Exception
	 */
	public void output(Tuple tuple) throws Exception;

	/**
	 * This method creates a branch of the current chain, inserting the specified
	 * actions in front of the actions in the rest of the chain.
	 * @param actions the actions to insert.
	 * @throws Exception
	 */
	public void branch(List<ActionConf> actions) throws Exception;

	/**
	 * This method creates a branch of the current chain, inserting the specified
	 * action in front of the actions in the rest of the chain.
	 * @param action the action to insert.
	 * @throws Exception
	 */
	public void branch(ActionConf action) throws Exception;

	public ActionOutput split(List<ActionConf> actions, int reconnectAt)
			throws Exception;

	public ActionOutput split(ActionConf action, int reconnectAt)
			throws Exception;
}
