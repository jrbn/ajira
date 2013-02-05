package nl.vu.cs.ajira.data.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import nl.vu.cs.ajira.utils.Consts;

public class TLongArray extends SimpleData {

	long[] array;

	public TLongArray(int size) {
		array = new long[size];
	}

	public TLongArray(long[] array) {
		this.array = array;
	}

	public TLongArray() {
	}

	public long[] getArray() {
		return array;
	}

	public void setArray(long[] array) {
		this.array = array;
	}

	@Override
	public int getIdDatatype() {
		return Consts.DATATYPE_TLONGARRAY;
	}

	@Override
	public void readFrom(DataInput input) throws IOException {
		int s = input.readInt();
		if (s != -1) {
			if (array == null || array.length != s) {
				array = new long[s];
			}
			for (int i = 0; i < s; ++i)
				array[i] = input.readLong();
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
			for (long v : array)
				output.writeLong(v);
		}
	}

	@Override
	public void copyTo(SimpleData el) {
		if (array != null) {
			((TLongArray) el).array = Arrays.copyOf(array, array.length);
		} else {
			((TLongArray) el).array = null;
		}
	}

	@Override
	public int compareTo(SimpleData el) {
		long[] array2 = ((TLongArray) el).array;
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
					long diff = array[i] - array2[i];
					if (diff != 0) {
						return (int) diff;
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
