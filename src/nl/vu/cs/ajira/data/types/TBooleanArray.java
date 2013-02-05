package nl.vu.cs.ajira.data.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import nl.vu.cs.ajira.utils.Consts;

public class TBooleanArray extends SimpleData {

	boolean[] array;

	public TBooleanArray(int size) {
		array = new boolean[size];
	}

	public TBooleanArray(boolean[] array) {
		this.array = array;
	}

	public TBooleanArray() {
	}

	public boolean[] getArray() {
		return array;
	}

	public void setArray(boolean[] array) {
		this.array = array;
	}

	@Override
	public int getIdDatatype() {
		return Consts.DATATYPE_TBOOLEANARRAY;
	}

	@Override
	public void readFrom(DataInput input) throws IOException {
		int s = input.readInt();
		if (s != -1) {
			if (array == null || array.length != s) {
				array = new boolean[s];
			}
			for (int i = 0; i < s; ++i)
				array[i] = input.readBoolean();
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
			for (boolean v : array)
				output.writeBoolean(v);
		}
	}

	@Override
	public void copyTo(SimpleData el) {
		if (array != null) {
			((TBooleanArray) el).array = Arrays.copyOf(array, array.length);
		} else {
			((TBooleanArray) el).array = null;
		}
	}

	@Override
	public int compareTo(SimpleData el) {
		boolean[] array2 = ((TBooleanArray) el).array;
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
					if (array[i] && !array2[i]) {
						return 1;
					} else if (!array[i] && array2[i]) {
						return -1;
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
