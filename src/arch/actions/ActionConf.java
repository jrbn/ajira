package arch.actions;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import arch.storage.Writable;

public class ActionConf extends Writable {

	private static final int MAX_PARAM_SIZE = 20;

	static class ParamItem {
		String name;
		boolean required;
		Object defaultValue;
	}

	static final Logger log = LoggerFactory.getLogger(PutIntoBucket.class);
	private final static Map<String, List<ActionConf.ParamItem>> actionParameters = new HashMap<String, List<ActionConf.ParamItem>>();

	private String className = "";
	private List<ActionConf.ParamItem> allowedParams = null;
	private Object[] currentValues = null;

	ActionConf() {
	}

	private ActionConf(String className,
			List<ActionConf.ParamItem> allowedParams) {
		if (allowedParams != null && className != null) {
			this.className = className;
			this.allowedParams = allowedParams;
			currentValues = new Object[allowedParams.size()];
		} else {
			currentValues = new Object[0];
			allowedParams = new ArrayList<>();
		}
	}

	private static boolean checkAllowedTypes(Object value) {
		if (value == null || value instanceof Integer || value instanceof Long
				|| value instanceof String || value instanceof Boolean) {
			return true;
		} else {
			return false;
		}
	}

	protected static boolean registerParameter(int id, String nameParam,
			Object defaultValue, boolean isRequired) {
		if (nameParam == null || !checkAllowedTypes(defaultValue)) {
			return false;
		}

		Class<?> currentClazz = Thread.currentThread().getStackTrace()[1]
				.getClass();
		synchronized (Action.class) {
			List<ActionConf.ParamItem> params = actionParameters
					.get(currentClazz.getName());
			if (params == null) {
				params = new ArrayList<ActionConf.ParamItem>();
				actionParameters.put(currentClazz.getName(), params);
			}
			ActionConf.ParamItem item = new ActionConf.ParamItem();
			item.name = nameParam;
			item.required = isRequired;
			if (params.size() < MAX_PARAM_SIZE) {
				params.add(id, item);
			} else {
				return false;
			}

		}
		return true;
	}

	public static ActionConf getActionConf() {
		Class<? extends Action> clazz = Thread.currentThread().getStackTrace()[1]
				.getClass().asSubclass(Action.class);
		return getActionConf(clazz);
	}

	public static ActionConf getActionConf(Class<? extends Action> clazz) {
		if (clazz == null)
			return null;

		return new ActionConf(clazz.getName(), actionParameters.get(clazz
				.getName()));
	}

	public void setParam(int pos, Object value) throws Exception {
		if (checkValidPos(pos)) {
			currentValues[pos] = value;
		} else {
			throw new Exception("Position not valid (" + pos + ")");
		}
	}

	private boolean checkValidPos(int pos) {
		return pos > 0 && pos < currentValues.length;
	}

	protected int getParamInt(int pos) throws Exception {
		if (currentValues[pos] == null) {
			Integer def = (Integer) allowedParams.get(pos).defaultValue;
			if (def == null) {
				throw new Exception(
						"The parameter is null and there is registred no default value");
			}
			return def;
		} else {
			return (Integer) getParam(pos);
		}

	}

	protected long getParamLong(int pos) throws Exception {
		if (currentValues[pos] == null) {
			Long def = (Long) allowedParams.get(pos).defaultValue;
			if (def == null) {
				throw new Exception(
						"The parameter is null and there is registred no default value");
			}
			return def;
		} else {
			return (Long) getParam(pos);
		}
	}

	protected boolean getParamBoolean(int pos) throws Exception {
		if (currentValues[pos] == null) {
			Boolean def = (Boolean) allowedParams.get(pos).defaultValue;
			if (def == null) {
				throw new Exception(
						"The parameter is null and there is registred no default value");
			}
			return def;
		} else {
			return (Boolean) getParam(pos);
		}
	}

	protected String getParamString(int pos) throws Exception {
		if (currentValues[pos] == null) {
			String def = (String) allowedParams.get(pos).defaultValue;
			if (def == null) {
				throw new Exception(
						"The parameter is null and there is registred no default value");
			}
			return def;
		} else {
			return (String) getParam(pos);
		}
	}

	protected Object getParam(int pos) throws Exception {
		if (!checkValidPos(pos)) {
			throw new Exception("Position not valid (" + pos + ")");
		}
		return currentValues[pos];
	}

	public String getClassName() {
		return className;
	}

	@Override
	public void readFrom(DataInput input) throws IOException {
		allowedParams = actionParameters.get(className);
		int size = input.readByte();
		currentValues = new Object[size];
		for (int i = 0; i < size; ++i) {
			int type = input.readByte();
			switch (type) {
			case 0:
				currentValues[i] = input.readInt();
				break;
			case 1:
				currentValues[i] = input.readUTF();
				break;
			case 2:
				currentValues[i] = input.readLong();
				break;
			case 3:
				currentValues[i] = input.readBoolean();
				break;
			}
		}
	}

	@Override
	public void writeTo(DataOutput output) throws IOException {
		output.writeByte(currentValues.length);
		for (int i = 0; i < currentValues.length; ++i) {
			Object value = currentValues[i];
			if (value != null) {
				if (value instanceof Integer) {
					output.writeByte(0);
					output.writeInt((int) value);
				} else if (value instanceof String) {
					output.writeByte(1);
					output.writeUTF((String) value);
				} else if (value instanceof Long) {
					output.writeByte(2);
					output.writeLong((Long) value);
				} else if (value instanceof Boolean) {
					output.writeByte(3);
					output.writeBoolean((boolean) value);
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
	public int bytesToStore() throws IOException {
		throw new IOException("Not (yet) implemented");
	}
}
