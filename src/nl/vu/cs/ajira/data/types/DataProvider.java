package nl.vu.cs.ajira.data.types;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import nl.vu.cs.ajira.storage.Factory;


public class DataProvider {

	static private DataProvider defaultInstance = null;

	private ArrayList<Factory<SimpleData>> list = new ArrayList<Factory<SimpleData>>();
	private static Map<Integer, Class<? extends SimpleData>> registeredTypes = new HashMap<Integer, Class<? extends SimpleData>>();

	/**
	 * 
	 * @param type is the id of the data type
	 * @param clazz is the class with the id type
	 */
	static public void addType(int type, Class<? extends SimpleData> clazz) {
		registeredTypes.put(type, clazz);
	}

	/**
	 * Constructs a new DataProvider and adds in the list 
	 * the classes that extend SimpleData and the elements
	 * from the map registeredTypes.
	 */
	public DataProvider() {
		list.add(new Factory<SimpleData>(TLong.class)); 	 // TLong has ID 0
		list.add(new Factory<SimpleData>(TString.class));	 // TString has ID 1
		list.add(new Factory<SimpleData>(TInt.class)); 		 // TInt has ID 2
		list.add(new Factory<SimpleData>(TSet.class)); 		 // TSet has ID 3
		list.add(new Factory<SimpleData>(TBoolean.class));   // TBoolean has ID 4
		list.add(new Factory<SimpleData>(TByte.class));		 // TByte has ID 5

		for (Map.Entry<Integer, Class<? extends SimpleData>> entry : registeredTypes
				.entrySet()) {
			list.add(entry.getKey(), new Factory<SimpleData>(entry.getValue()));
		}
	}
	
	/**
	 * 
	 * @return the defaultInstance. If the defaultInstance is null 
	 * it creates a new DataProvider.
	 */
	public static DataProvider getInstance() {
		if (defaultInstance == null) {
			defaultInstance = new DataProvider();
		}
		return defaultInstance;
	}

	/**
	 * 
	 * @param type is the id of the data type
	 * @return an object of the class with the id type 
	 */
	public SimpleData get(int type) {
		return list.get(type).get();
	}

	/**
	 * The object of the class data, from the list, is released.
	 * @param data is a SimpleData object
	 */
	public void release(SimpleData data) {
		list.get(data.getIdDatatype()).release(data);
	}
}