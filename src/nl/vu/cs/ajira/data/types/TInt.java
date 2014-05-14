package nl.vu.cs.ajira.data.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.utils.Consts;

/**
 * 
 * This class provides the methods that are needed in order to manipulate a int
 * value.
 * 
 */
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
	public TInt() {
	}

	/**
	 * Creates a new TInt and sets the filed value.
	 * 
	 * @param i
	 *            is the new value of the filed value
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
	 * 
	 * @param value
	 *            is the new value of the field value
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

	@Override
	/**
	 * Returns the hash value of the object.
	 */
	public int hashCode() {
		return value;
	}

	/**
	 * Converts the object to its string representation.
	 */
	@Override
	public String toString() {
		return Integer.toString(value);
	}

	@Override
	/**
	 * Copies the value of the current object in the parameters'value.
	 */
	public void copyTo(SimpleData el) {
		((TInt) el).value = value;
	}

	@Override
	/**
	 * Compares the value of two TInt objects.
	 * It returns 0 if the values of the objects are equal
	 * It returns a number greater than 0 if the value of the 
	 * current object is greater than the value of the parameter.
	 * It returns a number smaller than 0 if the value of the 
	 * current object is smaller than the value of the parameter.
	 */
	public int compareTo(SimpleData el) {
		return value - ((TInt) el).value;
	}

	@Override
	/**
	 * Compares the value of two TInt objects.
	 * It returns true if the value of the objects are equal
	 * It returns false if the value of the objects are different 
	 */
	public boolean equals(SimpleData el, ActionContext context) {
		return ((TInt) el).value == value;
	}
}