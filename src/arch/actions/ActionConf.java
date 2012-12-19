package arch.actions;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import arch.storage.Writable;

public class ActionConf extends Writable {

	static class ParamItem {
		String name;
		boolean required;
		Object defaultValue;
	}

	private static final int MAX_PARAM_SIZE = 20;

	private List<ParamItem> allowedParameters = null;
	private Object[] valuesParameters = null;
	private String className = null;

	ActionConf(String className, List<ParamItem> allowedParameters) {
		this.className = className;
		this.allowedParameters = allowedParameters;
		if (allowedParameters != null) {
			valuesParameters = new Object[allowedParameters.size()];
		}
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
			}
		}
		return valuesParameters;
	}

	private static boolean checkAllowedTypes(Object value) {
		if (value == null || value instanceof Integer || value instanceof Long
				|| value instanceof String || value instanceof Boolean) {
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

	public boolean validateParameters() {
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
}
