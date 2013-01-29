package nl.vu.cs.ajira.data.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import nl.vu.cs.ajira.utils.Consts;

public class TIntArray extends SimpleData {

	int[] array;

	public TIntArray(int size) {
		array = new int[size];
	}

	public TIntArray(int[] array) {
		this.array = array;
	}

	public TIntArray() {
	}

	public int[] getArray() {
		return array;
	}

	public void setArray(int[] array) {
		this.array = array;
	}

	@Override
	public int getIdDatatype() {
		return Consts.DATATYPE_TINTARRAY;
	}

	@Override
	public void readFrom(DataInput input) throws IOException {
		int s = input.readInt();
		if (s != -1) {
			if (array == null || array.length != s) {
				array = new int[s];
			}
			for (int i = 0; i < s; ++i)
				array[i] = input.readInt();
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
			for (int v : array)
				output.writeInt(v);
		}
	}

	@Override
	public int bytesToStore() throws IOException {
		return (array == null) ? 4 : 4 + array.length * 4;
	}

	@Override
	public void copyTo(SimpleData el) {
		if (array != null) {
			((TIntArray) el).array = Arrays.copyOf(array, array.length);
		} else {
			((TIntArray) el).array = null;
		}
	}

	@Override
	public int compareTo(SimpleData el) {
		int[] array2 = ((TIntArray) el).array;
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
					int diff = array[i] - array2[i];
					if (diff != 0) {
						return diff;
					}
					++i;
				}
				return array.length - array2.length;
			}
		}
	}
}
