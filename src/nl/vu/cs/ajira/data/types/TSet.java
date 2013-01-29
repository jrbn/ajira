package nl.vu.cs.ajira.data.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.TreeSet;

import nl.vu.cs.ajira.utils.Consts;


public final class TSet extends SimpleData {

	TreeSet<Long> values = new TreeSet<Long>();

	@Override
	/**
	 * Returns the id of data class type.
	 */
	public int getIdDatatype() {
		return Consts.DATATYPE_TSET;
	}
	
	/**
	 * Constructs a empty TreeSet
	 */
	public TSet() {
	}

	/**
	 * 
	 * @return the number of elements from the TreeSet
	 */
	public int getNElements() {
		return values.size();
	}
	
	/**
	 * 
	 * @return an Iterator for the TreeSet
	 */
	public Iterator<Long> getIterator() {
		return values.iterator();
	}
	
	/**
	 * Adds a new value to values.
	 * @param value is the value that will be added in the TreeSet values
	 */
	public void addValue(long value) {
		values.add(value);
	}

	/**
	 * Resets the field of the class.
	 */
	public void reset() {
		values.clear();
	}

	@Override
	/**
	 *  Reads from a DataInput the number of elements and then reads 
	 *  that elements and adds them into the TreeSet. 
	 */
	public void readFrom(DataInput input) throws IOException {
		values.clear();
		int nelements = input.readInt();
		for (int i = 0; i < nelements; ++i) {
			values.add(input.readLong());
		}
	}

	@Override
	/**
	 * Writes into a DataOutput the number of elements from the 
	 * set and then the elements.
	 */
	public void writeTo(DataOutput output) throws IOException {
		output.writeInt(values.size());
		for(long el : values) {
			output.writeLong(el);
		}
	}

	@Override
	/**
	 * Returns the number of bytes that are needed to store the
	 * field of the class.
	 */
	public int bytesToStore() {
		return 4 + 8 * values.size();
	}

}