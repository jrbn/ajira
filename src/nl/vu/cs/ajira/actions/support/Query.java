package nl.vu.cs.ajira.actions.support;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import nl.vu.cs.ajira.data.types.DataProvider;
import nl.vu.cs.ajira.data.types.SimpleData;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.data.types.TupleFactory;
import nl.vu.cs.ajira.storage.Writable;

public class Query implements Writable {

	private final Tuple tuple;

	public Query() {
		tuple = TupleFactory.newTuple();
	}

	public Query(Tuple tuple) {
		this.tuple = tuple;
	}

	public Query(SimpleData... data) {
		this();
		this.tuple.set(data);
	}

	public Query(String[] array) throws Exception {
		this();
		SimpleData[] data = new SimpleData[array.length];
		for (int i = 0; i < data.length; ++i)
			data[i] = (SimpleData) Class.forName(array[i]).newInstance();
	}

	public void setElements(SimpleData... data) {
		tuple.set(data);
	}

	public Tuple getTuple() {
		return tuple;
	}

	@Override
	public void readFrom(DataInput input) throws IOException {
		int nElements = input.readByte();
		SimpleData[] data = new SimpleData[nElements];
		for (int i = 0; i < nElements; ++i) {
			SimpleData el = DataProvider.getInstance().get(input.readByte());
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
