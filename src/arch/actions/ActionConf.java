package arch.actions;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import arch.ActionContext;
import arch.chains.Chain;
import arch.data.types.bytearray.BDataOutput;
import arch.storage.Writable;
import arch.utils.Consts;

public class ActionConf extends Writable {

	public static abstract class RuntimeParameterProcessor {
		public void process(Chain chain, ActionConf conf, ActionContext context) {
			processParameters(chain, conf.valuesParameters, context);
		}

		abstract void processParameters(Chain chain, Object[] params,
				ActionContext context);
	}

	static class ParamItem {
		String name;
		boolean required;
		Object defaultValue;
	}

	static final Logger log = LoggerFactory.getLogger(ActionConf.class);

	private static final int MAX_PARAM_SIZE = 20;

	private List<ParamItem> allowedParameters = null;
	private Object[] valuesParameters = null;
	private String className = null;
	private RuntimeParameterProcessor proc = null;

	ActionConf(String className, List<ParamItem> allowedParameters,
			RuntimeParameterProcessor proc) {
		this.className = className;
		this.allowedParameters = allowedParameters;
		if (allowedParameters != null) {
			valuesParameters = new Object[allowedParameters.size()];
		}
		this.proc = proc;
	}

	final static Object[] readValuesFromStream(DataInput input)
			throws IOException {
		int size = input.readByte();
		Object[] valuesParameters = new Object[size];
		for (int i = 0; i < size; ++i) {
			int type = input.readByte();
			switch (type) {
			case 0:
				valuesParameters[i] = input.readInt();
				break;
			case 1:
				valuesParameters[i] = input.readUTF();
				break;
			case 2:
				valuesParameters[i] = input.readLong();
				break;
			case 3:
				valuesParameters[i] = input.readBoolean();
				break;
			case 4:
				byte[] b = new byte[input.readInt()];
				input.readFully(b);
				valuesParameters[i] = b;
				break;
			}
		}
		return valuesParameters;
	}

	private static boolean checkAllowedTypes(Object value) {
		if (value == null || value instanceof Integer || value instanceof Long
				|| value instanceof String || value instanceof Boolean
				|| value instanceof Writable) {
			return true;
		} else {
			return false;
		}
	}

	public void registerParameter(int id, String nameParam,
			Object defaultValue, boolean isRequired) throws Exception {
		if (nameParam == null || !checkAllowedTypes(defaultValue)) {
			throw new Exception("Not valid parameter");
		}

		if (allowedParameters == null) {
			allowedParameters = new ArrayList<ParamItem>();
		}

		if (allowedParameters.size() >= MAX_PARAM_SIZE) {
			throw new Exception("Reached maximum number of parameters ("
					+ MAX_PARAM_SIZE + ")");
		}

		ParamItem item = new ParamItem();
		item.name = nameParam;
		item.required = isRequired;
		item.defaultValue = defaultValue;
		allowedParameters.add(id, item);
	}

	void registerRuntimeParameterProcessor(
			Class<? extends RuntimeParameterProcessor> proc) {
		try {
			this.proc = proc.newInstance();
		} catch (Exception e) {
			log.error("Failed in creating the RuntimeParameterProcessor");
		}
	}

	List<ParamItem> getListAllowedParameters() {
		return allowedParameters;
	}

	@Override
	public final void readFrom(DataInput input) throws IOException {
		valuesParameters = readValuesFromStream(input);
	}

	@Override
	public final void writeTo(DataOutput output) throws IOException {
		output.writeByte(valuesParameters.length);
		for (int i = 0; i < valuesParameters.length; ++i) {
			Object value = valuesParameters[i];
			if (value != null) {
				if (value instanceof Integer) {
					output.writeByte(0);
					output.writeInt(((Integer) value).intValue());
				} else if (value instanceof String) {
					output.writeByte(1);
					output.writeUTF((String) value);
				} else if (value instanceof Long) {
					output.writeByte(2);
					output.writeLong((Long) value);
				} else if (value instanceof Boolean) {
					output.writeByte(3);
					output.writeBoolean(((Boolean) value).booleanValue());
				} else if (value instanceof Writable) {
					output.writeByte(4);
					BDataOutput o = new BDataOutput(new byte[Consts.CHAIN_SIZE]);
					((Writable) value).writeTo(o);
					output.writeInt(o.cb.end);
					output.write(o.cb.buffer, 0, o.cb.end);
				} else {
					throw new IOException(
							"Format of one parameter is not recognized");
				}
			} else {
				// Check to see whether there is a required field
				output.writeByte(-1);
			}
		}
	}

	@Override
	public final int bytesToStore() throws IOException {
		throw new IOException("Not (yet) implemented");
	}

	public final void setParam(int pos, Object value) throws Exception {
		if (valuesParameters == null) {
			throw new Exception("No parameters are allowed");
		}
		if (pos < 0 || pos >= valuesParameters.length) {
			throw new Exception("Position not valid (" + pos + ")");
		}

		if (!checkAllowedTypes(value)) {
			throw new Exception("Object type not valid");
		}

		valuesParameters[pos] = value;
	}

	public String getClassName() {
		return className;
	}

	public final boolean validateParameters() {
		if (allowedParameters != null) {
			for (int i = 0; i < allowedParameters.size(); ++i) {
				ParamItem item = allowedParameters.get(i);
				if (valuesParameters[i] == null) {
					if (item.required)
						return false;
					if (item.defaultValue != null) {
						valuesParameters[i] = item.defaultValue;
					}
				}
			}
		}
		return true;
	}

	public boolean isParProcessorDefined() {
		return proc != null;
	}

	public RuntimeParameterProcessor getRuntimeParametersProcessor() {
		return proc;
	}
}
