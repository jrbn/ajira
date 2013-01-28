package nl.vu.cs.ajira.data.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import nl.vu.cs.ajira.utils.Consts;

public final class TBoolean extends SimpleData {

	boolean value;

	public TBoolean(boolean b) {
		value = b;
	}

	public TBoolean() {
	}

	public boolean getValue() {
		return value;
	}

	public void setValue(boolean value) {
		this.value = value;
	}

	@Override
	public int getIdDatatype() {
		return Consts.DATATYPE_TBOOLEAN;
	}

	@Override
	public void readFrom(DataInput input) throws IOException {
		value = input.readBoolean();
	}

	@Override
	public void writeTo(DataOutput output) throws IOException {
		output.writeBoolean(value);
	}

	@Override
	public int bytesToStore() {
		return 1;
	}

	@Override
	public String toString() {
		return Boolean.toString(value);
	}

	@Override
	public void copyTo(SimpleData el) {
		((TBoolean) el).value = value;
	}

	@Override
	public int compareTo(SimpleData el) {
		if ((value && ((TBoolean) el).value)
				|| (!value && !((TBoolean) el).value)) {
			return 0;
		} else if (value) {
			return 1;
		} else {
			return -1;
		}
	}
}
