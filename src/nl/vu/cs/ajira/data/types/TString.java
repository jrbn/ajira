package nl.vu.cs.ajira.data.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import nl.vu.cs.ajira.storage.RawComparator;
import nl.vu.cs.ajira.utils.Consts;

public final class TString extends SimpleData {

	String value = null;

	/**
	 * It creates a new TString.
	 * 
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
	 * 
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
		if (size >= 0) {
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
			output.writeInt(-1);
		} else {
			byte[] b = value.getBytes();
			output.writeInt(b.length);
			output.write(b, 0, b.length);
		}
	}

	/**
	 * Converts the object to its string representation.
	 */
	@Override
	public String toString() {
		return value;
	}

	@Override
	/**
	 * Returns the hash value of the object.
	 */
	public int hashCode() {
		return value.hashCode();
	}

	@Override
	/**
	 * Copies the value of the current object in the parameters'value.
	 */
	public void copyTo(SimpleData el) {
		((TString) el).value = value;
	}

	@Override
	/**
	 * Compares lexicographically the value of two TString objects.
	 * It returns 0 if the values of the objects are equal
	 * It returns a number greater than 0 if the value of the 
	 * current object is greater than the value of the parameter.
	 * It returns a number smaller than 0 if the value of the 
	 * current object is smaller than the value of the parameter.
	 */
	public int compareTo(SimpleData el) {
		return value.compareTo(((TString) el).value);
	}

	@Override
	/**
	 * Compares the value of two TString objects.
	 * It returns true if the value of the objects are equal
	 * It returns false if the value of the objects are different 
	 */
	public boolean equals(SimpleData el) {
		return ((TString) el).value.equals(value);
	}

	/**
	 * Initialize the static members of the class.
	 */
	static {
		RawComparator.registerComparator(Consts.DATATYPE_TSTRING,
				new RawComparator<SimpleData>() {
					@Override
					public int compare(byte[] b1, int s1, int l1, byte[] b2,
							int s2, int l2) {
						return super.compare(b1, s1 + 4, l1 - 4, b2, s2 + 4,
								l2 - 4);
					}
				});
	}
}
