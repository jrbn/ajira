package nl.vu.cs.ajira.actions.support;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

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

	public void setElements(SimpleData... data) {
		tuple.set(data);
	}

	public Tuple getTuple() {
		return tuple;
	}

	@Override
	public void readFrom(DataInput input) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void writeTo(DataOutput output) throws IOException {
		// TODO Auto-generated method stub

	}
}
