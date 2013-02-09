package nl.vu.cs.ajira.data.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import nl.vu.cs.ajira.utils.Consts;

import com.google.common.primitives.Longs;

public final class TLong extends SimpleData {

	protected long value;

	@Override
	/**
	 * Returns the id of data class type.
	 */
	public int getIdDatatype() {
		return Consts.DATATYPE_TLONG;
	}

	/**
	 * Creates a new TLong and sets the filed value.
	 * 
	 * @param value
	 *            is the new value of the filed value
	 */
	public TLong(long value) {
		this.value = value;
	}

	/**
	 * Creates a empty TLong.
	 */
	public TLong() {
	}

	/**
	 * 
	 * @return the value of the filed value.
	 */
	public long getValue() {
		return value;
	}

	/**
	 * Sets the field value.
	 * 
	 * @param value
	 *            is the new value of the field value
	 */
	public void setValue(long value) {
		this.value = value;
	}

	@Override
	/**
	 * Reads a long value from a DataInput.
	 */
	public void readFrom(DataInput input) throws IOException {
		value = input.readLong();
	}

	@Override
	/**
	 * Writes a long value in a DataOutput.
	 */
	public void writeTo(DataOutput output) throws IOException {
		output.writeLong(value);
	}

	@Override
	public int hashCode() {
		return Longs.hashCode(value);

	}

	@Override
	/**
	 * Converts the object to its string representation.
	 */
	public String toString() {
		return Long.toString(value);
	}

	@Override
	public void copyTo(SimpleData el) {
		((TLong) el).value = value;
	}

	@Override
	public int compareTo(SimpleData el) {
		return (int) (value - ((TLong) el).value);
	}

	@Override
	public boolean equals(SimpleData el) {
		return ((TLong) el).value == value;
	}
}