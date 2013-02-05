package nl.vu.cs.ajira.data.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import nl.vu.cs.ajira.utils.Consts;

public final class TInt extends SimpleData {

	int value;

	@Override
	/**
	 * Returns the id of data class type.
	 */
	public int getIdDatatype() {
		return Consts.DATATYPE_TINT;
	}
	
	/**
	 * Creates an empty TInt.
	 */
	public TInt() {}
	
	/**
	 * Creates a new TInt and sets the filed value. 
	 * @param i is the new value of the filed value
	 */
	public TInt(int i) {
		value = i;
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
	 * Reads one int from a DataInput.
	 */
	public void readFrom(DataInput input) throws IOException {
		value = input.readInt();
	}

	@Override
	/**
	 * Writes one int in a DataOutput.
	 */
	public void writeTo(DataOutput output) throws IOException {
		output.writeInt(value);
	}

	/**
	 * Returns the number of bytes that are needed to store the
	 * field of the class.
	 */
	// @Override
	// public int bytesToStore() {
	// return 4;
	// }

	/**
	 * Converts the object to its string representation.
	 */
	public String toString() {
		return Integer.toString(value);
	}

	@Override
	public void copyTo(SimpleData el) {
		((TInt) el).value = value;
	}

	@Override
	public int compareTo(SimpleData el) {
		return value - ((TInt) el).value;
	}

	@Override
	public boolean equals(SimpleData el) {
		return ((TInt) el).value == value;
	}
}