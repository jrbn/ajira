package nl.vu.cs.ajira.data.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.utils.Consts;

public class TByte extends SimpleData {

	int value;

	/**
	 * Creates a new TByte and sets the filed value.
	 * 
	 * @param b
	 *            is the new value of the filed value
	 */
	public TByte(int b) {
		value = b;
	}

	/**
	 * Creates an empty TByte.
	 */
	public TByte() {
	}

	@Override
	/**
	 * Returns the id of data class type.
	 */
	public int getIdDatatype() {
		return Consts.DATATYPE_TBYTE;
	}

	@Override
	/**
	 * Reads one byte from a DataInput.
	 */
	public void readFrom(DataInput input) throws IOException {
		value = input.readByte();
	}

	@Override
	/**
	 * Writes value in a DataOutput.
	 */
	public void writeTo(DataOutput output) throws IOException {
		output.writeByte(value);
	}

	@Override
	/**
	 * Returns the hash value of the object
	 */
	public int hashCode() {
		return value;
	}

	/**
	 * 
	 * @return the value of the filed value.
	 */
	public int getValue() {
		return value;
	}

	/**
	 * Sets the field value.
	 * 
	 * @param value
	 *            is the new value of the field value
	 */
	public void setValue(int value) {
		this.value = value;
	}

	@Override
	/**
	 * Converts the object to its string representation.
	 */
	public String toString() {
		return Integer.toString(value);
	}

	@Override
	/**
	 * Copies the value of the current object in the parameters'value.
	 */
	public void copyTo(SimpleData el) {
		((TByte) el).value = value;
	}

	@Override
	/**
	 * Compares the value of two TByte objects.
	 * It returns 0 if the values of the objects are equal
	 * It returns a number greater than 0 if the value of the 
	 * current object is greater than the value of the parameter.
	 * It returns a number smaller than 0 if the value of the 
	 * current object is smaller than the value of the parameter.
	 */
	public int compareTo(SimpleData el) {
		return value - ((TByte) el).value;
	}

	@Override
	/**
	 * Compares the value of two TByte objects.
	 * It returns true if the value of the objects are equal
	 * It returns false if the value of the objects are different 
	 */
	public boolean equals(SimpleData el, ActionContext context) {
		return ((TByte) el).value == value;
	}
}
