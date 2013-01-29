package nl.vu.cs.ajira.data.types;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import nl.vu.cs.ajira.storage.Factory;
import nl.vu.cs.ajira.utils.Consts;

public class DataProvider {

	static private DataProvider defaultInstance = null;

	private ArrayList<Factory<SimpleData>> list = new ArrayList<Factory<SimpleData>>();
	private static Map<Integer, Class<? extends SimpleData>> registeredTypes = new HashMap<Integer, Class<? extends SimpleData>>();
	private static Map<String, Integer> retrieveIds = new HashMap<>();

	static synchronized public void addType(int type,
			Class<? extends SimpleData> clazz) {
		registeredTypes.put(type, clazz);
		retrieveIds.put(clazz.getName(), type);
	}

	public DataProvider() {
		list.add(new Factory<SimpleData>(TLong.class)); // TLong has ID 0
		list.add(new Factory<SimpleData>(TString.class)); // TString has ID 1
		list.add(new Factory<SimpleData>(TInt.class)); // TInt has ID 2
		list.add(new Factory<SimpleData>(TBag.class)); // TSet has ID 3
		list.add(new Factory<SimpleData>(TBoolean.class)); // TBoolean has ID 4
		list.add(new Factory<SimpleData>(TByte.class)); // TByte has ID 5
		list.add(new Factory<SimpleData>(TByteArray.class)); // TByteArray has
																// ID 6
		list.add(new Factory<SimpleData>(TIntArray.class)); // TIntArray has ID
															// 7
		list.add(new Factory<SimpleData>(TStringArray.class)); // TStringArray
																// has
																// ID 8

		retrieveIds.put(TLong.class.getName(), Consts.DATATYPE_TLONG);
		retrieveIds.put(TInt.class.getName(), Consts.DATATYPE_TINT);
		retrieveIds.put(TBoolean.class.getName(), Consts.DATATYPE_TBOOLEAN);
		retrieveIds.put(TBag.class.getName(), Consts.DATATYPE_TBAG);
		retrieveIds.put(TByte.class.getName(), Consts.DATATYPE_TBYTE);
		retrieveIds.put(TString.class.getName(), Consts.DATATYPE_TSTRING);
		retrieveIds.put(TByteArray.class.getName(), Consts.DATATYPE_TBYTEARRAY);
		retrieveIds.put(TIntArray.class.getName(), Consts.DATATYPE_TINTARRAY);
		retrieveIds.put(TStringArray.class.getName(),
				Consts.DATATYPE_TSTRINGARRAY);

		for (Map.Entry<Integer, Class<? extends SimpleData>> entry : registeredTypes
				.entrySet()) {
			list.add(entry.getKey(), new Factory<SimpleData>(entry.getValue()));
		}
	}

	public static DataProvider getInstance() {
		if (defaultInstance == null) {
			defaultInstance = new DataProvider();
		}
		return defaultInstance;
	}

	public static int getId(String className) {
		return retrieveIds.get(className);
	}

	public SimpleData get(int type) {
		return list.get(type).get();
	}

	public void release(SimpleData data) {
		list.get(data.getIdDatatype()).release(data);
	}
}