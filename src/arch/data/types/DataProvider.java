package arch.data.types;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import arch.storage.Factory;

public class DataProvider {

	static private Map<Integer, Class<? extends SimpleData>> registredTypes = new HashMap<Integer, Class<? extends SimpleData>>();

	static public void addType(int type, Class<? extends SimpleData> clazz) {
		registredTypes.put(type, clazz);
	}

	ArrayList<Factory<SimpleData>> list = new ArrayList<Factory<SimpleData>>();

	public DataProvider() {
		list.add(new Factory<SimpleData>(TLong.class)); // TLong has ID 0
		list.add(new Factory<SimpleData>(TString.class)); // TString has ID 1
		list.add(new Factory<SimpleData>(TInt.class)); // TInt has ID 2
		list.add(new Factory<SimpleData>(TSet.class)); // TSet has ID 3
		list.add(new Factory<SimpleData>(TBoolean.class)); // TBoolean has ID 4
		list.add(new Factory<SimpleData>(TByte.class)); // TByte has ID 5

		for (Map.Entry<Integer, Class<? extends SimpleData>> entry : registredTypes
				.entrySet()) {
			list.add(entry.getKey(), new Factory<SimpleData>(entry.getValue()));
		}

	}

	public SimpleData get(int type) {
		return list.get(type).get();
	}

	public void release(SimpleData data) {
		list.get(data.getIdDatatype()).release(data);
	}
}