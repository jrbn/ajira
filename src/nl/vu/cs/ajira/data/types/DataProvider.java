package nl.vu.cs.ajira.data.types;

import java.util.HashMap;
import java.util.Map;

import nl.vu.cs.ajira.storage.Factory;
import nl.vu.cs.ajira.utils.Consts;

public class DataProvider {

	static private DataProvider defaultInstance = null;

	@SuppressWarnings("unchecked")
	private Factory<SimpleData>[] list = new Factory[256];
	private static Map<Integer, Class<? extends SimpleData>> registeredTypes = new HashMap<Integer, Class<? extends SimpleData>>();
	private static Map<String, Integer> retrieveIds = new HashMap<>();

	static synchronized public void addType(int type,
			Class<? extends SimpleData> clazz) {
		registeredTypes.put(type, clazz);
		retrieveIds.put(clazz.getName(), type);
	}

	public DataProvider() {
		list[Consts.DATATYPE_TLONG] = new Factory<SimpleData>(TLong.class);
		list[Consts.DATATYPE_TSTRING] = new Factory<SimpleData>(TString.class);
		list[Consts.DATATYPE_TINT] = new Factory<SimpleData>(TInt.class);
		list[Consts.DATATYPE_TBAG] = new Factory<SimpleData>(TBag.class);
		list[Consts.DATATYPE_TBOOLEAN] = new Factory<SimpleData>(TBoolean.class);
		list[Consts.DATATYPE_TBYTE] = new Factory<SimpleData>(TByte.class);
		list[Consts.DATATYPE_TBYTEARRAY] = new Factory<SimpleData>(
				TByteArray.class);
		list[Consts.DATATYPE_TINTARRAY] = new Factory<SimpleData>(
				TIntArray.class);
		list[Consts.DATATYPE_TSTRINGARRAY] = new Factory<SimpleData>(
				TStringArray.class);
		list[Consts.DATATYPE_TLONGARRAY] = new Factory<SimpleData>(
				TLongArray.class);
		list[Consts.DATATYPE_TBOOLEANARRAY] = new Factory<SimpleData>(
				TBooleanArray.class);

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
		retrieveIds.put(TLongArray.class.getName(), Consts.DATATYPE_TLONGARRAY);
		retrieveIds.put(TBooleanArray.class.getName(),
				Consts.DATATYPE_TBOOLEANARRAY);

		for (Map.Entry<Integer, Class<? extends SimpleData>> entry : registeredTypes
				.entrySet()) {
			list[entry.getKey()] = new Factory<SimpleData>(entry.getValue());
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
		return list[type].get();
	}

	public void release(SimpleData data) {
		list[data.getIdDatatype()].release(data);
	}
}