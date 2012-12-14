package arch.actions;

import arch.ActionContext;
import arch.chains.Chain;
import arch.data.types.Tuple;
import arch.data.types.bytearray.BDataInput;
import arch.storage.Writable;
import arch.storage.container.WritableContainer;

public abstract class Action {

	private Object[] valuesParameters = null;

	/***** ACTION PROCESSING ******/

	public void setupActionParameters(ActionConf conf) throws Exception {
	}

	public boolean blockProcessing() {
		return false;
	}

	public void startProcess(ActionContext context, Chain chain)
			throws Exception {
	}

	public abstract void process(ActionContext context, Chain chain,
			Tuple inputTuple, WritableContainer<Tuple> output,
			WritableContainer<Chain> chainsToProcess) throws Exception;

	public void stopProcess(ActionContext context, Chain chain,
			WritableContainer<Tuple> output,
			WritableContainer<Chain> chainsToProcess) throws Exception {
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

		if (valuesParameters[pos] instanceof byte[])
			throw new Exception(
					"This parameter is of type Writable. Should be invoked using getParamWritable");
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

}