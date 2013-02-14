package nl.vu.cs.ajira.actions;

import nl.vu.cs.ajira.data.types.TIntArray;
import nl.vu.cs.ajira.data.types.TStringArray;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.data.types.bytearray.BDataInput;
import nl.vu.cs.ajira.storage.Writable;

public abstract class Action {

	private Object[] valuesParameters = null;

	/***** ACTION PROCESSING ******/

	public void registerActionParameters(ActionConf conf) throws Exception {
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

	protected final int getParamInt(int pos) throws Exception {
		Object obj = getParam(pos);
		if (obj == null) {
			throw new Exception("The value is null");
		}
		return (Integer) getParam(pos);
	}

	protected final long getParamLong(int pos) throws Exception {
		Object obj = getParam(pos);
		if (obj == null) {
			throw new Exception("The value is null");
		}
		return (Long) getParam(pos);
	}

	protected final boolean getParamBoolean(int pos) throws Exception {
		Object obj = getParam(pos);
		if (obj == null) {
			throw new Exception("The value is null");
		}
		return (Boolean) obj;
	}

	protected final String getParamString(int pos) throws Exception {
		return (String) getParam(pos);
	}

	protected final Object getParam(int pos) throws Exception {
		if (valuesParameters == null) {
			throw new Exception(
					"The parameters are not specified. Was this action created incorrectly?");
		}

		if (pos < 0 || pos >= valuesParameters.length) {
			throw new Exception("Position not valid (" + pos + ")");
		}

		return valuesParameters[pos];
	}

	protected final void getParamWritable(Writable b, int pos) throws Exception {
		if (valuesParameters == null) {
			throw new Exception(
					"The parameters are not specified. Was this action created incorrectly?");
		}

		if (pos < 0 || pos >= valuesParameters.length) {
			throw new Exception("Position not valid (" + pos + ")");
		}

		b.readFrom(new BDataInput((byte[]) valuesParameters[pos]));
	}

	protected final byte[] getParamByteArray(int pos) throws Exception {
		Object o = getParam(pos);
		if (o != null) {
			return (byte[]) o;
		}
		return null;
	}

	protected final int[] getParamIntArray(int pos) throws Exception {
		Object o = getParam(pos);
		if (o != null) {
			return ((TIntArray) o).getArray();
		}
		return null;
	}

	protected final String[] getParamStringArray(int pos) throws Exception {
		Object o = getParam(pos);
		if (o != null) {
			return ((TStringArray) o).getArray();
		}
		return null;
	}
}