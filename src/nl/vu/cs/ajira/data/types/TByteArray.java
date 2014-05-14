package nl.vu.cs.ajira.data.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.storage.RawComparator;
import nl.vu.cs.ajira.utils.Consts;

/**
 * 
 * This class provides the methods that are needed in order to manipulate an
 * array of byte values.
 * 
 */
public class TByteArray extends SimpleData {

	byte[] array;

	/**
	 * Creates an empty TLongArray object.
	 */
	public TByteArray() {
	}

	/**
	 * Creates a TLongArray object with a byte[] as backend.
	 */
	public TByteArray(byte[] array) {
		this.array = array;
	}

	/**
	 * 
	 * @return the field of the class
	 */
	public byte[] getArray() {
		return array;
	}

	/**
	 * Sets the field of the class.
	 * 
	 * @param array
	 *            is the new array of the object
	 */
	public void setArray(byte[] array) {
		this.array = array;
	}

	@Override
	/**
	 * Returns the id of the class.
	 */
	public int getIdDatatype() {
		return Consts.DATATYPE_TBYTEARRAY;
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
				array = new byte[s];
			}
			input.readFully(array);
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
			output.write(array);
		}
	}

	@Override
	/**
	 * Copies the array of the current object in the parameters'array.
	 */
	public void copyTo(SimpleData el) {
		if (array != null) {
			((TByteArray) el).array = Arrays.copyOf(array, array.length);
		} else {
			((TByteArray) el).array = null;
		}
	}

	@Override
	/**
	 * Compares the fields of two TByteArray objects.
	 * Returns 0 in case of equality, 
	 * Returns a number lower than 0 in case the current array is lower than the parameter's array, 
	 * Returns a number greater than 0 in case the current array is greater than the parameter's array
	 */
	public int compareTo(SimpleData el) {
		byte[] array2 = ((TByteArray) el).array;
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
				return RawComparator.compareBytes(array, 0, array.length,
						array2, 0, array2.length);
			}
		}
	}

	@Override
	/**
	 * Compares two TByteArray objects.
	 * It returns true if they are equal
	 * It returns false if they are not equal 
	 */
	public boolean equals(SimpleData el, ActionContext context) {
		return compareTo(el) == 0;
	}

	@Override
	public int hashCode() {
		return Arrays.hashCode(array);
	}
}
