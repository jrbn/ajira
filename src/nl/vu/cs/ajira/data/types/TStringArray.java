package nl.vu.cs.ajira.data.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import nl.vu.cs.ajira.utils.Consts;

public class TStringArray extends SimpleData {

	String[] array;

	public TStringArray(String[] array) {
		this.array = array;
	}

	public TStringArray() {
	}

	public String[] getArray() {
		return array;
	}

	public void setArray(String[] array) {
		this.array = array;
	}

	@Override
	public int getIdDatatype() {
		return Consts.DATATYPE_TSTRINGARRAY;
	}

	@Override
	public void readFrom(DataInput input) throws IOException {
		int s = input.readInt();
		if (s != -1) {
			if (array == null || array.length != s) {
				array = new String[s];
			}
			for (int i = 0; i < s; ++i)
				array[i] = input.readUTF();
		} else {
			array = null;
		}
	}

	@Override
	public void writeTo(DataOutput output) throws IOException {
		if (array == null) {
			output.writeInt(-1);
		} else {
			output.writeInt(array.length);
			for (String v : array)
				output.writeUTF(v);
		}
	}

	// @Override
	// public int bytesToStore() throws IOException {
	// throw new IOException("Not (yet) implemented");
	// }

	@Override
	public void copyTo(SimpleData el) {
		if (array != null) {
			((TStringArray) el).array = Arrays.copyOf(array, array.length);
		} else {
			((TStringArray) el).array = null;
		}
	}

	@Override
	public int compareTo(SimpleData el) {
		String[] array2 = ((TStringArray) el).array;
		if (array == null) {
			if (array2 == null) {
				return 0;
			} else {
				return -1;
			}
		} else {
			if (array2 == null) {
				return 1;
			} else {
				int i = 0;
				while (i < array.length && i < array2.length) {
					int diff = array[i].compareTo(array2[i]);
					if (diff != 0) {
						return diff;
					}
					++i;
				}
				return array.length - array2.length;
			}
		}
	}

	@Override
	public boolean equals(SimpleData el) {
		return compareTo(el) == 0;
	}
}
