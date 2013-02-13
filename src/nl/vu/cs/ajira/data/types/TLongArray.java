package nl.vu.cs.ajira.data.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import nl.vu.cs.ajira.utils.Consts;

public class TLongArray extends SimpleData {

	long[] array;

	/**
	 * Creates a new TLongArray and sets the size of its field.
	 * @param size is the size of the array
	 */
	public TLongArray(int size) {
		array = new long[size];
	}

	/**
	 * Creates a new TLongArray and sets its field.
	 * @param array is the new array of the object
	 */
	public TLongArray(long[] array) {
		this.array = array;
	}
	
	/**
	 * Creates an empty TLongArray object. 
	 */
	public TLongArray() {
	}

	/**
	 * 
	 * @return the field of the class
	 */
	public long[] getArray() {
		return array;
	}

	/**
	 * Sets the field of the class.
	 * 
	 * @param array is the new array of the object
	 */
	public void setArray(long[] array) {
		this.array = array;
	}

	@Override
	/**
	 * Returns the id of the class.
	 */
	public int getIdDatatype() {
		return Consts.DATATYPE_TLONGARRAY;
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
				array = new long[s];
			}
			for (int i = 0; i < s; ++i)
				array[i] = input.readLong();
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
			for (long v : array)
				output.writeLong(v);
		}
	}

	@Override
	/**
	 * Copies the array of the current object in the parameters'array.
	 */
	public void copyTo(SimpleData el) {
		if (array != null) {
			((TLongArray) el).array = Arrays.copyOf(array, array.length);
		} else {
			((TLongArray) el).array = null;
		}
	}

	@Override
	/**
	 * Compares two TLongArray objects.
	 * 
	 * Returns 0 in case of equality, 
	 * Returns a number lower than 0 in case the current array is lower than the parameter's array, 
	 * Returns a number greater than 0 in case the current array is greater than the parameter's array
	 */
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
	/**
	 * Compares two TLongArray objects.
	 * It returns true if they are equal
	 * It returns false if they are not equal 
	 */
	public boolean equals(SimpleData el) {
		return compareTo(el) == 0;
	}
}
