package nl.vu.cs.ajira.data.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import nl.vu.cs.ajira.utils.Consts;

public final class TBoolean extends SimpleData {

	boolean value;

	/**
	 * Creates a new TBoolean and sets the filed value.
	 * 
	 * @param b is the new value of the filed value
	 */
	public TBoolean(boolean b) {
		value = b;
	}

	/**
	 * Creates an empty TBoolean.
	 */
	public TBoolean() {
	}

	/**
	 * 
	 * @return the value of the filed value.
	 */
	public boolean getValue() {
		return value;
	}

	/**
	 * Sets the field value.
	 * 
	 * @param value is the new value of the field value
	 */
	public void setValue(boolean value) {
		this.value = value;
	}

	@Override
	/**
	 * Returns the id of data class type.
	 */
	public int getIdDatatype() {
		return Consts.DATATYPE_TBOOLEAN;
	}

	@Override
	/**
	 * Reads one boolean value from a DataInput.
	 */
	public void readFrom(DataInput input) throws IOException {
		value = input.readBoolean();
	}

	@Override
	/**
	 * Writes value in a DataOutput.
	 */
	public void writeTo(DataOutput output) throws IOException {
		output.writeBoolean(value);
	}

	@Override
	/**
	 * Returns the hash value of the object. If the value 
	 * is true than it returns 1, else it return 0.
	 */
	public int hashCode() {
		return value ? 1 : 0;
	}

	@Override
	/**
	 * Converts the object to its string representation.
	 */
	public String toString() {
		return Boolean.toString(value);
	}

	@Override
	/**
	 * Copies the value of the current object in the parameters'value.
	 */
	public void copyTo(SimpleData el) {
		((TBoolean) el).value = value;
	}

	@Override
	/**
	 * Compares the value of two TBoolean objects.
	 * It returns 0 if the values of the objects are equal
	 * It returns 1 if the value of the current object is true
	 * It returns -1 if the value of the current object is false 
	 */
	public int compareTo(SimpleData el) {
		if ((value && ((TBoolean) el).value)
				|| (!value && !((TBoolean) el).value)) {
			return 0;
		} else if (value) {
			return 1;
		} else {
			return -1;
		}
	}

	@Override
	/**
	 * Compares the value of two TBoolean objects.
	 * It returns true if the value of the objects are equal
	 * It returns false if the value of the objects are different 
	 */
	public boolean equals(SimpleData el) {
		return ((TBoolean) el).value == value;
	}

}
