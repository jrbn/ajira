package nl.vu.cs.ajira.data.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import nl.vu.cs.ajira.utils.Consts;

public final class TString extends SimpleData {

	String value = null;

	public TString(String value) {
		this.value = value;
	}

	public TString() {
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	@Override
	public int getIdDatatype() {
		return Consts.DATATYPE_TSTRING;
	}

	@Override
	public void readFrom(DataInput input) throws IOException {
		int size = input.readInt();
		if (size > 0) {
			byte[] nvalues = new byte[size];
			input.readFully(nvalues, 0, size);
			value = new String(nvalues);
		} else {
			value = null;
		}
	}

	@Override
	public void writeTo(DataOutput output) throws IOException {
		if (value == null) {
			output.writeInt(0);
		} else {
			byte[] b = value.getBytes();
			output.writeInt(b.length);
			output.write(b, 0, b.length);
		}
	}

	// @Override
	// public int bytesToStore() {
	// return value == null ? 4 : value.getBytes().length + 4;
	// }

	@Override
	public String toString() {
		return value;
	}

	@Override
	public void copyTo(SimpleData el) {
		((TString) el).value = value;
	}

	@Override
	public int compareTo(SimpleData el) {
		return value.compareTo(((TString) el).value);
	}

	@Override
	public boolean equals(SimpleData el) {
		return ((TString) el).value.equals(value);
	}
}
