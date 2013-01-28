package nl.vu.cs.ajira.data.types;

public class TupleFactory {

	public static final TupleFactory instance = new TupleFactory();

	private TupleFactory() {
	}

	public static Tuple newTuple() {
		return new Tuple();
	}

	public static Tuple newTuple(SimpleData... data) {
		return new Tuple(data);
	}

}
