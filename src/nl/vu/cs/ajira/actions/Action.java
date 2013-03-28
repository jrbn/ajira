package nl.vu.cs.ajira.actions;

import java.io.IOException;

import nl.vu.cs.ajira.data.types.TIntArray;
import nl.vu.cs.ajira.data.types.TStringArray;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.data.types.bytearray.BDataInput;
import nl.vu.cs.ajira.storage.Writable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The <code>Action</code> class is the base class for all actions. An action is
 * the building block for Ajira. The heart of an action is its
 * {@link #process(Tuple, ActionContext, ActionOutput)} method, which processes
 * the input data and provides the data for the next action in the chain. Before
 * processing the data, {@link #startProcess(ActionContext)} is called, once,
 * and when the data is exhausted,
 * {@link #stopProcess(ActionContext, ActionOutput)} is called. Also, actions
 * can be configured with action-specific parameters, through the
 * {@link #registerActionParameters(ActionConf)} method.
 * <p>
 * This abstract class provides default (empty) implementations for
 * {@link #registerActionParameters(ActionConf)},
 * {@link #startProcess(ActionContext)}, and
 * {@link #stopProcess(ActionContext, ActionOutput)}. There is no default
 * implementation for {@link #process(Tuple, ActionContext, ActionOutput)};
 * An action should do something, after all.
 * <p>
 * Ajira provides various actions, but actions can be user-specified as
 * well, see the {@link nl.vu.cs.ajira.examples} package.
 */
public abstract class Action {

	/** The configuration parameters of this action. */
	private Object[] valuesParameters = null;

	/** Logging. */
	protected static final Logger log = LoggerFactory.getLogger(Action.class);

	/***** ACTION PROCESSING ******/

	/**
	 * Stores the parameter specifications in the {@link ActionConf} object, by
	 * calling the {@link ActionConf#registerParameter(int, String, Object, boolean)}
	 * method for each configuration parameter. Also, a custom parameter configurator
	 * can be installed, by calling
	 * {@link ActionConf#registerCustomConfigurator(ActionConf.Configurator)}.
	 * 
	 * @param conf
	 *            the action configuration.
	 */
	protected void registerActionParameters(ActionConf conf) {
		// empty default implementation.
	}

	/**
	 * Initialization of the process.
	 * 
	 * @param context
	 *            context in which this action is executed.
	 * @throws Exception
	 */
	public void startProcess(ActionContext context) throws Exception {
		// empty default implementation.
	}

	/**
	 * This method is called once for each input tuple (specified by the first parameter).
	 * Output tuples are passed on to the {@link ActionOutput#output(Tuple)} or the
	 * {@link ActionOutput#output(nl.vu.cs.ajira.data.types.SimpleData...)}
	 * method.
	 * 
	 * @param tuple
	 *            the input tuple.
	 * @param context
	 *            the context in which this action is executed.
	 * @param actionOutput
	 *            to pass on the result of processing the input tuple.
	 * @throws Exception
	 */
	public abstract void process(Tuple tuple, ActionContext context,
			ActionOutput actionOutput) throws Exception;

	/**
	 * Overriding this method allows for finishing up the process.
	 * 
	 * @param context
	 *            the context in which this action is executed.
	 * @param actionOutput
	 *            to pass on possible left-overs.
	 * @throws Exception
	 */
	public void stopProcess(ActionContext context, ActionOutput actionOutput)
			throws Exception {
		// empty default implementation.
	}

	/* PARAMETERS MANAGEMENT */

	final void setParams(Object[] params) {
		this.valuesParameters = params;
	}

	/**
	 * Obtains the parameter, supposed to be of type <code>integer</code>, at
	 * the specified position.
	 * 
	 * @param pos
	 *            the index of the required parameter.
	 * @return the value of the parameter.
	 * @throws IllegalArgumentException
	 *             when there are no parameters, or the specified position is
	 *             out of range, or the parameter is null.
	 * @throws ClassCastException
	 *             when the specified parameter is of the wrong type.
	 */
	protected final int getParamInt(int pos) {
		Object obj = getParam(pos);
		if (obj == null) {
			throw new IllegalArgumentException("The value is null");
		}
		return (Integer) getParam(pos);
	}

	/**
	 * Obtains the parameter, supposed to be of type <code>long</code>, at the
	 * specified position.
	 * 
	 * @param pos
	 *            the index of the required parameter.
	 * @return the value of the parameter.
	 * @throws IllegalArgumentException
	 *             when there are no parameters, or the specified position is
	 *             out of range, or the parameter is null.
	 * @throws ClassCastException
	 *             when the specified parameter is of the wrong type.
	 */
	protected final long getParamLong(int pos) {
		Object obj = getParam(pos);
		if (obj == null) {
			throw new IllegalArgumentException("The value is null");
		}
		return (Long) getParam(pos);
	}

	/**
	 * Obtains the parameter, supposed to be of type <code>boolean</code>, at
	 * the specified position.
	 * 
	 * @param pos
	 *            the index of the required parameter.
	 * @return the value of the parameter.
	 * @throws IllegalArgumentException
	 *             when there are no parameters, or the specified position is
	 *             out of range, or the parameter is null.
	 * @throws ClassCastException
	 *             when the specified parameter is of the wrong type.
	 */
	protected final boolean getParamBoolean(int pos) {
		Object obj = getParam(pos);
		if (obj == null) {
			throw new IllegalArgumentException("The value is null");
		}
		return (Boolean) obj;
	}

	/**
	 * Obtains the parameter, supposed to be of type <code>String</code>, at the
	 * specified position.
	 * 
	 * @param pos
	 *            the index of the required parameter.
	 * @return the value of the parameter.
	 * @throws IllegalArgumentException
	 *             when there are no parameters, or the specified position is
	 *             out of range.
	 * @throws ClassCastException
	 *             when the specified parameter is of the wrong type.
	 */
	protected final String getParamString(int pos) {
		return (String) getParam(pos);
	}

	/**
	 * Returns the value of the parameter at the specified position.
	 * 
	 * @param pos
	 *            the index of the required parameter.
	 * @return the value of the parameter.
	 * @throws IllegalArgumentException
	 *             when there are no parameters, or the specified position is
	 *             out of range.
	 */
	protected final Object getParam(int pos) {
		if (valuesParameters == null) {
			throw new IllegalArgumentException(
					"The parameters are not specified. Was this action created incorrectly?");
		}

		if (pos < 0 || pos >= valuesParameters.length) {
			throw new IllegalArgumentException("Position not valid (" + pos
					+ ")");
		}

		return valuesParameters[pos];
	}

	/**
	 * Stores the value of the {@link Writable} at the specified position.
	 * 
	 * @param b
	 *            the {@link Writable} to store into.
	 * @param pos
	 *            the index of the required parameter.
	 * @throws IllegalArgumentException
	 *             when there are no parameters, or the specified position is
	 *             out of range, or an unexpected <code>Writable</code> type is
	 *             specified.
	 * @throws ClassCastException
	 *             when the specified parameter is of the wrong type.
	 */
	protected final void getParamWritable(Writable b, int pos) {
		Object obj = getParam(pos);
		try {
			if (obj != null)
				b.readFrom(new BDataInput((byte[]) obj));
		} catch (IOException e) {
			throw new IllegalArgumentException(
					"Unmarshalling exception; possibly a wrong Writable type?",
					e);
		}
	}

	/**
	 * Obtains the parameter, supposed to be of type <code>byte[]</code>, at the
	 * specified position.
	 * 
	 * @param pos
	 *            the index of the required parameter.
	 * @return the value of the parameter.
	 * @throws IllegalArgumentException
	 *             when there are no parameters, or the specified position is
	 *             out of range.
	 * @throws ClassCastException
	 *             when the specified parameter is of the wrong type.
	 */
	protected final byte[] getParamByteArray(int pos) {
		Object o = getParam(pos);
		return (byte[]) o;
	}

	/**
	 * Obtains the parameter, supposed to be of type <code>int[]</code>, at the
	 * specified position.
	 * 
	 * @param pos
	 *            the index of the required parameter.
	 * @return the value of the parameter.
	 * @throws IllegalArgumentException
	 *             when there are no parameters, or the specified position is
	 *             out of range.
	 * @throws ClassCastException
	 *             when the specified parameter is of the wrong type.
	 */
	protected final int[] getParamIntArray(int pos) {
		Object o = getParam(pos);
		if (o != null) {
			return ((TIntArray) o).getArray();
		}
		return null;
	}

	/**
	 * Obtains the parameter, supposed to be of type <code>String[]</code>, at
	 * the specified position.
	 * 
	 * @param pos
	 *            the index of the required parameter.
	 * @return the value of the parameter.
	 * @throws IllegalArgumentException
	 *             when there are no parameters, or the specified position is
	 *             out of range.
	 * @throws ClassCastException
	 *             when the specified parameter is of the wrong type.
	 */
	protected final String[] getParamStringArray(int pos) {
		Object o = getParam(pos);
		if (o != null) {
			return ((TStringArray) o).getArray();
		}
		return null;
	}
}