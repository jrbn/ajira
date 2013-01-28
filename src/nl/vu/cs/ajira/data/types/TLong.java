package nl.vu.cs.ajira.data.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import nl.vu.cs.ajira.utils.Consts;

public final class TLong extends SimpleData {

	protected long value;

	@Override
	public int getIdDatatype() {
		return Consts.DATATYPE_TLONG;
	}

	public TLong(long value) {
		this.value = value;
	}

	public TLong() {
	}

	public long getValue() {
		return value;
	}

	public void setValue(long value) {
		this.value = value;
	}

	@Override
	public void readFrom(DataInput input) throws IOException {
		value = input.readLong();
	}

	@Override
	public void writeTo(DataOutput output) throws IOException {
		output.writeLong(value);
	}

	@Override
	public int bytesToStore() {
		return 8;
	}

	@Override
	public String toString() {
		return Long.toString(value);
	}

	@Override
	public void copyTo(SimpleData el) {
		((TLong) el).value = value;
	}

	@Override
	public int compareTo(SimpleData el) {
		return (int) (value - ((TLong) el).value);
	}
}