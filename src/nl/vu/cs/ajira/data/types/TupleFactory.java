package nl.vu.cs.ajira.data.types;

public class TupleFactory {

	public static final TupleFactory instance = new TupleFactory();

	/**
	 * Creates a new TupleFactory.
	 */
	private TupleFactory() {
	}

	/**
	 * 
	 * @return a new Tuple
	 */
	public static Tuple newTuple() {
		return new Tuple();
	}

	/**
	 * 
	 * @param data is a array of SimpleData objects
	 * @returns new Tuple and adds the elements from the parameter to the Tuple
	 */
	public static Tuple newTuple(SimpleData... data) {
		return new Tuple(data);
	}

}
