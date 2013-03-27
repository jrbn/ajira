package nl.vu.cs.ajira.data.types;

/**
 * This class provides some convenience methods for constructing tuples.
 */
public class TupleFactory {

	/**
	 * Prevent creation of an instance.
	 */
	private TupleFactory() {
	}

	/**
	 * This method creates a new {@link Tuple} instance.
	 * @return
	 * 		a new Tuple
	 */
	public static Tuple newTuple() {
		return new Tuple();
	}

	/**
	 * This method creates a new {@link Tuple} instance, initialized with the specified
	 * data.
	 * @param data
	 *		the data to initialize the tuple with
	 * @returns
	 * 		the new, initialized tuple
	 */
	public static Tuple newTuple(SimpleData... data) {
		return new Tuple(data);
	}

}
