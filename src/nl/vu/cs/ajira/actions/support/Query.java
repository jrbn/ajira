package nl.vu.cs.ajira.actions.support;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import nl.vu.cs.ajira.data.types.DataProvider;
import nl.vu.cs.ajira.data.types.SimpleData;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.data.types.TupleFactory;
import nl.vu.cs.ajira.storage.Writable;

/**
 * Data structure used for the parameter specification of a chain of actions.
 */
public class Query implements Writable {

	private final Tuple tuple;

	/**
	 * Constructs a new <code>Query</code> with empty content.
	 */
	public Query() {
		tuple = TupleFactory.newTuple();
	}

	/**
	 * Constructs a new <code>Query</code> with the specified tuple as content.
	 * @param tuple
	 *		the contents of this <code>Query</code> object
	 */
	public Query(Tuple tuple) {
		this.tuple = tuple;
	}

	/**
	 * Constructs a new <code>Query</code> with the specified data as content.
	 * @param data
	 * 		the contents of this <code>Query</code> object
	 */
	public Query(SimpleData... data) {
		this();
		setElements(data);
	}

	/**
	 * Constructs a new <code>Query</code> with the specified data types as content.
	 * @param array
	 * 		a list of class names, specifying the data types of the content
	 */
	public Query(String[] array) throws Exception {
		this();
		SimpleData[] data = new SimpleData[array.length];
		for (int i = 0; i < data.length; ++i)
			data[i] = (SimpleData) Class.forName(array[i]).newInstance();
		setElements(data);
	}

	/**
	 * Sets the content of this <code>Query</code> object to the specified data.
	 * @param data
	 * 		the data to store into this <code>Query</code>
	 */
	public void setElements(SimpleData... data) {
		tuple.set(data);
	}

	/**
	 * Obtains the content of the <code>Query</code> object, as a {@link Tuple}.
	 * @return
	 * 		the content
	 */
	public Tuple getTuple() {
		return tuple;
	}

	@Override
	public void readFrom(DataInput input) throws IOException {
		int nElements = input.readByte();
		SimpleData[] data = new SimpleData[nElements];
		for (int i = 0; i < nElements; ++i) {
			SimpleData el = DataProvider.get().get(input.readByte());
			el.readFrom(input);
			data[i] = el;
		}
		tuple.set(data);
	}

	@Override
	public void writeTo(DataOutput output) throws IOException {
		output.writeByte(tuple.getNElements());
		for (int i = 0; i < tuple.getNElements(); ++i) {
			SimpleData el = tuple.get(i);
			output.writeByte(el.getIdDatatype());
			el.writeTo(output);
		}
	}
}
