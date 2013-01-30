package nl.vu.cs.ajira.buckets;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.storage.Writable;

public class SerializedTuple implements Writable {

	private boolean shouldSort;
	private byte[] fieldsToSort;
	private int nFields;
	private Tuple tuple;

	public SerializedTuple(byte[] fieldsToSort, int nFields) {
		shouldSort = true;
		this.fieldsToSort = fieldsToSort;
		this.nFields = nFields;
	}

	public SerializedTuple() {
	}

	public SerializedTuple(Tuple tuple) {
		this.tuple = tuple;
	}

	public void setTuple(Tuple tuple) {
		this.tuple = tuple;
	}

	public Tuple getTuple() {
		return tuple;
	}

	@Override
	public void readFrom(DataInput input) throws IOException {
		for (int i = 0; i < tuple.getNElements(); ++i) {
			tuple.get(i).readFrom(input);
		}
	}

	@Override
	public void writeTo(DataOutput output) throws IOException {
		for (int i = 0; i < tuple.getNElements(); ++i) {
			tuple.get(i).writeTo(output);
		}
	}

	// @Override
	// public int bytesToStore() throws IOException {
	// int size = 0;
	// for (int i = 0; i < tuple.getNElements(); ++i) {
	// if (tuple.get(i) != null)
	// size += tuple.get(i).bytesToStore();
	// }
	// return size;
	// }
}
