package nl.vu.cs.ajira.data.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import nl.vu.cs.ajira.utils.Consts;


public class TByte extends SimpleData {

	int value;

	/**
	 * Creates a new TByte and sets the filed value. 
	 * @param b is the new value of the filed value
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
	 * Returns the number of bytes that are needed to store the
	 * field of the class.
	 */
	public int bytesToStore() {
		return 1;
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
	 * @param value is the new value of the field value
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
}
