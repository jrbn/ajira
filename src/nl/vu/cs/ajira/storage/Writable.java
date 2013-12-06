package nl.vu.cs.ajira.storage;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Objects that implement the <code>Writable</code> interface can be read from
 * a {@link DataInput} object, and written to a {@link DataOutput} object.
 * TODO: investigate: replace with java.io.Externalizable?
 */
public interface Writable {

	/**
	 * The object implements the <code>readFrom</code> method to restore its contents
	 * by reading its fields from the specified stream.
	 * @param input
	 * 		the stream to read from
	 * @throws IOException
	 * 		includes any I/O exceptions that may occur
	 */
	public void readFrom(DataInput input) throws IOException;

	/**
	 * The object implements the <code>writeTo</code> method to save its contents
	 * by writing its fields to the specified stream.
	 * @param output
	 * 		the stream to write to
	 * @throws IOException
	 * 		includes any I/O exceptions that may occur
	 */
	public void writeTo(DataOutput output) throws IOException;
}
