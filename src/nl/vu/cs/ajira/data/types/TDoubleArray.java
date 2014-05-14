package nl.vu.cs.ajira.data.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.utils.Consts;

public class TDoubleArray extends SimpleData {

	private double[] array;

	@Override
	public void readFrom(DataInput input) throws IOException {
		int s = input.readInt();
		array = new double[s];
		for (int i = 0; i < s; ++i) {
			array[i] = input.readDouble();
		}
	}

	@Override
	public void writeTo(DataOutput output) throws IOException {
		output.writeInt(array.length);
		for (int i = 0; i < array.length; ++i) {
			output.writeDouble(array[i]);
		}
	}

	public void setArray(double[] array) {
		this.array = array;
	}

	public double[] getArray() {
		return array;
	}

	@Override
	public int getIdDatatype() {
		return Consts.DATATYPE_TDOUBLEARRAY;
	}

	@Override
	public void copyTo(SimpleData el) {
		if (array != null) {
			((TDoubleArray) el).array = Arrays.copyOf(array, array.length);
		} else {
			((TDoubleArray) el).array = null;
		}
	}

	@Override
	public int compareTo(SimpleData el) {
		double[] array2 = ((TDoubleArray) el).array;
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
					double diff = array[i] - array2[i];
					if (diff < 0) {
						return -1;
					} else if (diff > 0) {
						return 1;
					}
					++i;
				}
				return array.length - array2.length;
			}
		}
	}

	@Override
	public boolean equals(SimpleData el, ActionContext context) {
		return compareTo(el) == 0;
	}

	/**
	 * Converts the object to its string representation.
	 */
	@Override
	public String toString() {
		return Arrays.toString(array);
	}

	@Override
	public int hashCode() {
		return Arrays.hashCode(array);
	}
}
