package nl.vu.cs.ajira.actions;

import nl.vu.cs.ajira.data.types.SimpleData;
import nl.vu.cs.ajira.data.types.Tuple;

/**
 * This interface defines the methods that can be used to make actions produce
 * results. Ajira passes an <code>ActionOutput</code> object on to the
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
	 * @param tuple the data to pass on.
	 * @throws Exception
	 */
	public void output(Tuple tuple) throws Exception;

	/**
	 * This method creates a branch of the current chain, inserting the specified
	 * actions in front of the actions in the rest of the chain.
	 * @param actions the actions to insert.
	 * @throws Exception
	 */
	public void branch(ActionConf... actions) throws Exception;

	/**
	 * This method creates a split of the current chain, returning a separate <code>ActionOutput</code>
	 * to produce results to, in addition to the current one (for the current chain). At some point
	 * the chains will merge again, as determined by the <code>reconnectAt</code> parameter.
	 * @param reconnectAt
	 *		determines after how many actions in the current chain the split
	 *		will merge with the current chain.
	 * @param actions
	 * 		the actions to be executed by the split.
	 * @return the <code>ActionOutput</code> for the split.
	 * @throws Exception
	 */
	public ActionOutput split(int reconnectAt, ActionConf... actions)
			throws Exception;
}
