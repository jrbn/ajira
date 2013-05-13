package nl.vu.cs.ajira.data.types;

import java.util.HashMap;
import java.util.Map;

import nl.vu.cs.ajira.storage.Factory;
import nl.vu.cs.ajira.utils.Consts;

public class DataProvider {

	static private DataProvider defaultInstance = null;

	private static int counter = 15;
	@SuppressWarnings("unchecked")
	private final Factory<SimpleData>[] list = new Factory[256];
	private static Map<Integer, Class<? extends SimpleData>> registeredTypes = new HashMap<Integer, Class<? extends SimpleData>>();
	private static Map<String, Integer> retrieveIds = new HashMap<String, Integer>();

	/**
	 * 
	 * @param type
	 *            is the id of the clazz
	 * @param clazz
	 *            is the class whose informations are added at the fields of the
	 *            DataProvider
	 */
	static synchronized public void addType(Class<? extends SimpleData> clazz) {
		registeredTypes.put(counter, clazz);
		retrieveIds.put(clazz.getName(), counter++);
	}

	/**
	 * Constructs a new DataProvider and adds in the list the classes that
	 * extend SimpleData and the elements from the map registeredTypes. It also
	 * sets the informations from the retriveIds. It adds the name and the id of
	 * the class.
	 */
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
		list[Consts.DATATYPE_TDOUBLE] = new Factory<SimpleData>(TDouble.class);
		list[Consts.DATATYPE_TDOUBLEARRAY] = new Factory<SimpleData>(
				TDoubleArray.class);

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
		retrieveIds.put(TDouble.class.getName(), Consts.DATATYPE_TDOUBLE);
		retrieveIds.put(TDoubleArray.class.getName(),
				Consts.DATATYPE_TDOUBLEARRAY);

		for (Map.Entry<Integer, Class<? extends SimpleData>> entry : registeredTypes
				.entrySet()) {
			list[entry.getKey()] = new Factory<SimpleData>(entry.getValue());
		}
	}

	/**
	 * 
	 * @return the defaultInstance. If the defaultInstance is null it creates a
	 *         new DataProvider.
	 */
	public static DataProvider get() {
		if (defaultInstance == null) {
			defaultInstance = new DataProvider();
		}
		return defaultInstance;
	}

	/**
	 * 
	 * @param className
	 *            is the name of the class
	 * @return the id of the class with the name className
	 */
	public static int getId(String className) {
		return retrieveIds.get(className);
	}

	/**
	 * 
	 * @param type
	 *            is the id of the object's class that is looked
	 * @return the object that is found at the index type in the list
	 */
	public SimpleData get(int type) {
		return list[type].get();
	}

	/**
	 * Releases in the Factory's buffer the object which is found in the list at
	 * the data's id position.
	 * 
	 * @param data
	 *            is the object that will be released in the Factory's buffer
	 */
	public void release(SimpleData data) {
		list[data.getIdDatatype()].release(data);
	}
}