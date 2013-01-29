package nl.vu.cs.ajira.data.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import nl.vu.cs.ajira.utils.Consts;


public final class TString extends SimpleData {

	String value = null;

	/**
	 * It creates a new TString.
	 * @param value is the new value of the field value
	 */
	public TString(String value) {
		this.value = value;
	}
	/**
	 * It creates an empty TString. 
	 */
	public TString() {
	}

	/**
	 * 
	 * @return the value of the filed value.
	 */
	public String getValue() {
		return value;
	}
	/**
	 * Sets the field value.
	 * @param value is the new value of the field value
	 */
	public void setValue(String value) {
		this.value = value;
	}

	@Override
	/**
	 * returns the id of data class type.
	 */
	public int getIdDatatype() {
		return Consts.DATATYPE_TSTRING;
	}

	@Override
	/**
	 * Reads from a DataInput the size and then size bytes that are used 
	 * to create a new String for the field value.
	 */
	public void readFrom(DataInput input) throws IOException {
		int size = input.readInt();
		if (size > 0) {
			byte[] nvalues = new byte[size];
			input.readFully(nvalues, 0, size);
			value = new String(nvalues);
		} else {
			value = null;
		}
	}

	@Override
	/**
	 * Writes the length and then the value into a DataOutput.
	 */
	public void writeTo(DataOutput output) throws IOException {
		if (value == null) {
			output.writeInt(0);
		} else {
			byte[] b = value.getBytes();
			output.writeInt(b.length);
			output.write(b, 0, b.length);
		}
	}

	@Override
	/**
	 * Returns the number of bytes that are needed to store the
	 * fields of the class.
	 */
	public int bytesToStore() {
		return value == null ? 4 : value.getBytes().length + 4;
	}

	/**
	 * Converts the object to its string representation.
	 */
	public String toString() {
		return value;
	}

}
