package nl.vu.cs.ajira.submissions;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import nl.vu.cs.ajira.storage.Writable;

public class JobProperties implements Writable, Serializable {

	private final Map<String, String> properties = new HashMap<String, String>();

	public void putProperty(String prop, String val) {
		properties.put(prop, val);
	}

	public String getProperty(String prop, String defaultValue) {
		if (properties.containsKey(prop)) {
			return properties.get(prop);
		}
		return defaultValue;
	}

	public String getProperty(String prop) {
		return properties.get(prop);
	}

	@Override
	public void readFrom(DataInput input) throws IOException {
		properties.clear();
		int n = input.readInt();
		for (int i = 0; i < n; i++) {
			String key = input.readUTF();
			String value = input.readUTF();
			properties.put(key, value);
		}
	}

	@Override
	public void writeTo(DataOutput output) throws IOException {
		output.writeInt(properties.size());
		for (Map.Entry<String, String> entry : properties.entrySet()) {
			output.writeUTF(entry.getKey());
			output.writeUTF(entry.getValue());
		}
	}

	public int size() {
		return properties.size();
	}
}
