package nl.vu.cs.ajira.actions;

import nl.vu.cs.ajira.data.types.TIntArray;
import nl.vu.cs.ajira.data.types.TStringArray;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.data.types.bytearray.BDataInput;
import nl.vu.cs.ajira.storage.Writable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class Action {

	private Object[] valuesParameters = null;
	private Logger log = LoggerFactory.getLogger(Action.class);

	/***** ACTION PROCESSING ******/

	public void registerActionParameters(ActionConf conf) {
	}

	public void startProcess(ActionContext context) throws Exception {
	}

	public abstract void process(Tuple tuple, ActionContext context,
			ActionOutput actionOutput) throws Exception;

	public void stopProcess(ActionContext context, ActionOutput actionOutput)
			throws Exception {
	}

	/***** PARAMETERS MANAGEMENT ******/

	final void setParams(Object[] params) {
		this.valuesParameters = params;
	}

	protected final int getParamInt(int pos) {
		Object obj = getParam(pos);
		if (obj == null) {
			throw new IllegalArgumentException("The value is null");
		}
		return (Integer) getParam(pos);
	}

	protected final long getParamLong(int pos) {
		Object obj = getParam(pos);
		if (obj == null) {
			throw new IllegalArgumentException("The value is null");
		}
		return (Long) getParam(pos);
	}

	protected final boolean getParamBoolean(int pos) {
		Object obj = getParam(pos);
		if (obj == null) {
			throw new IllegalArgumentException("The value is null");
		}
		return (Boolean) obj;
	}

	protected final String getParamString(int pos) {
		return (String) getParam(pos);
	}

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

	protected final void getParamWritable(Writable b, int pos) {
		if (valuesParameters == null) {
			throw new IllegalArgumentException(
					"The parameters are not specified. Was this action created incorrectly?");
		}

		if (pos < 0 || pos >= valuesParameters.length) {
			throw new IllegalArgumentException("Position not valid (" + pos
					+ ")");
		}

		try {
			if (valuesParameters[pos] != null)
				b.readFrom(new BDataInput((byte[]) valuesParameters[pos]));
		} catch (Exception e) {
			// Should not happen.
			log.error("Got unexpected IOException", e);
		}
	}

	protected final byte[] getParamByteArray(int pos) {
		Object o = getParam(pos);
		if (o != null) {
			return (byte[]) o;
		}
		return null;
	}

	protected final int[] getParamIntArray(int pos) {
		Object o = getParam(pos);
		if (o != null) {
			return ((TIntArray) o).getArray();
		}
		return null;
	}

	protected final String[] getParamStringArray(int pos) {
		Object o = getParam(pos);
		if (o != null) {
			return ((TStringArray) o).getArray();
		}
		return null;
	}
}