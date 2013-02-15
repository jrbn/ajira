package nl.vu.cs.ajira.data.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import nl.vu.cs.ajira.utils.Consts;

public class TIntArray extends SimpleData {

	int[] array;

	/**
	 * Creates a new TIntArray and sets the size of its field.
	 * @param size is the size of the array
	 */
	public TIntArray(int size) {
		array = new int[size];
	}

	/**
	 * Creates a new TIntArray and sets its field.
	 * @param array is the new array of the object
	 */
	public TIntArray(int[] array) {
		this.array = array;
	}

	/**
	 * Creates an empty TIntArray object. 
	 */
	public TIntArray() {
	}
	
	/**
	 * 
	 * @return the field of the class
	 */
	public int[] getArray() {
		return array;
	}

	/**
	 * Sets the field of the class.
	 * 
	 * @param array is the new array of the object
	 */
	public void setArray(int[] array) {
		this.array = array;
	}

	@Override
	/**
	 * Returns the id of the class.
	 */
	public int getIdDatatype() {
		return Consts.DATATYPE_TINTARRAY;
	}

	@Override
	/**
	 * Reads from a DataInput the size of the array and then 
	 * the corresponding number of boolean values.  
	 */
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
	/**
	 * If the array is not null it writes in a DataOutput the size 
	 * of the array and then its values. If the array is null then 
	 * it writes -1. 
	 */
	public void writeTo(DataOutput output) throws IOException {
		if (array == null) {
			output.writeInt(-1);
		} else {
			output.writeInt(array.length);
			for (int v : array)
				output.writeInt(v);
		}
	}

	// @Override
	// public int bytesToStore() throws IOException {
	// return (array == null) ? 4 : 4 + array.length * 4;
	// }

	@Override
	/**
	 * Copies the array of the current object in the parameters'array.
	 */
	public void copyTo(SimpleData el) {
		if (array != null) {
			((TIntArray) el).array = Arrays.copyOf(array, array.length);
		} else {
			((TIntArray) el).array = null;
		}
	}

	@Override
	/**
	 * Compares two TIntArray objects.
	 * 
	 * Returns 0 in case of equality, 
	 * Returns a number lower than 0 in case the current array is lower than the parameter's array, 
	 * Returns a number greater than 0 in case the current array is greater than the parameter's array
	 */
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

	@Override
	/**
	 * Compares two TIntArray objects.
	 * It returns true if they are equal
	 * It returns false if they are not equal 
	 */
	public boolean equals(SimpleData el) {
		return compareTo(el) == 0;
	}
}
