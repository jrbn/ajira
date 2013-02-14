package nl.vu.cs.ajira.data.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import nl.vu.cs.ajira.utils.Consts;

public class TBooleanArray extends SimpleData {

	boolean[] array;

	/**
	 * Creates a new TBooleanArray and sets the size of its field.
	 * @param size is the size of the array
	 */
	public TBooleanArray(int size) {
		array = new boolean[size];
	}

	/**
	 * Creates a new TBooleanArray and sets its field.
	 * @param array is the new array of the object
	 */
	public TBooleanArray(boolean[] array) {
		this.array = array;
	}

	/**
	 * Creates a empty TBooleanArray. 
	 */
	public TBooleanArray() {
	}

	/**
	 * 
	 * @return the field of the class
	 */
	public boolean[] getArray() {
		return array;
	}

	/**
	 * Sets the field of the class.
	 * 
	 * @param array is the new array of the object
	 */
	public void setArray(boolean[] array) {
		this.array = array;
	}

	@Override
	/**
	 * Returns the id of the class.
	 */
	public int getIdDatatype() {
		return Consts.DATATYPE_TBOOLEANARRAY;
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
				array = new boolean[s];
			}
			for (int i = 0; i < s; ++i)
				array[i] = input.readBoolean();
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
			for (boolean v : array)
				output.writeBoolean(v);
		}
	}

	@Override
	/**
	 * Copies the array of the current object in the parameters'array.
	 */
	public void copyTo(SimpleData el) {
		if (array != null) {
			((TBooleanArray) el).array = Arrays.copyOf(array, array.length);
		} else {
			((TBooleanArray) el).array = null;
		}
	}

	@Override
	/**
	 * Compares the fields of two TBooleanArray objects.
	 * It returns 0 if the the objects are equal
	 * It returns 1 if an element from the current object array is true 
	 * and the corresponding element of the parameter's array is false or 
	 * the parameter's array is shorter, but all of its elements are equal with
	 * the elements from the current array.   
	 * It returns -1 if an element from the current object array is false 
	 * and the corresponding element of the parameter's array is true or 
	 * the current array is shorter, but all of its elements are equal with
	 * the elements from the parameter's array.
	 */
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
	/**
	 * Compares two TBooleanArray objects.
	 * It returns true if they are equal
	 * It returns false if they are not equal 
	 */
	public boolean equals(SimpleData el) {
		return compareTo(el) == 0;
	}
}
