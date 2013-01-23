package nl.vu.cs.ajira.data.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.TreeSet;

import nl.vu.cs.ajira.utils.Consts;


public final class TSet extends SimpleData {

	TreeSet<Long> values = new TreeSet<Long>();

	@Override
	public int getIdDatatype() {
		return Consts.DATATYPE_TSET;
	}

	public TSet() {
	}

	public int getNElements() {
		return values.size();
	}
	
	public Iterator<Long> getIterator() {
		return values.iterator();
	}

	public void addValue(long value) {
		values.add(value);
	}

	public void reset() {
		values.clear();
	}

	@Override
	public void readFrom(DataInput input) throws IOException {
		values.clear();
		int nelements = input.readInt();
		for (int i = 0; i < nelements; ++i) {
			values.add(input.readLong());
		}
	}

	@Override
	public void writeTo(DataOutput output) throws IOException {
		output.writeInt(values.size());
		for(long el : values) {
			output.writeLong(el);
		}
	}

	@Override
	public int bytesToStore() {
		return 4 + 8 * values.size();
	}

}