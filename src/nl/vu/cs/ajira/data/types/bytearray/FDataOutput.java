package nl.vu.cs.ajira.data.types.bytearray;

import java.io.DataOutput;
import java.io.IOException;
import java.io.OutputStream;

public class FDataOutput implements DataOutput {

	OutputStream os;

	/**
	 * Creates a new FDataOutput and sets the field of the class.
	 * @param os is the new OutputStream of the class
	 */
	public FDataOutput(OutputStream os) {
		this.os = os;
	}

	/**
	 * Closes the OutStream of the class.
	 * @throws IOException
	 */
	public void close() throws IOException {
		os.close();
	}

	@Override
	/**
	 * Writes b to the OutputStream.
	 */
	public void write(int b) throws IOException {
		os.write(b);
	}

	@Override
	/**
	 * Writes the array b to the OutputStream.
	 */
	public void write(byte[] b) throws IOException {
		os.write(b);
	}

	@Override
	/**
	 * Writes len bytes starting with the position off from
	 * the array b to the OutputStream.
	 */
	public void write(byte[] b, int off, int len) throws IOException {
		os.write(b, off, len);
	}

	@Override
	/**
	 * Writes the boolean value b to the OutputStream.
	 */
	public void writeBoolean(boolean v) throws IOException {
		if (v) {
			os.write(1);
		} else {
			os.write(0);
		}
	}

	@Override
	/**
	 * Writes one byte from the int value v to the OutputStream.
	 */
	public void writeByte(int v) throws IOException {
		os.write(v);
	}

	@Override
	public void writeBytes(String s) throws IOException {
		throw new IOException("Not supported");
	}

	@Override
	public void writeChar(int v) throws IOException {
		throw new IOException("Not supported");
	}

	@Override
	public void writeChars(String s) throws IOException {
		throw new IOException("Not supported");
	}

	@Override
	public void writeDouble(double v) throws IOException {
		throw new IOException("Not supported");
	}

	@Override
	public void writeFloat(float v) throws IOException {
		throw new IOException("Not supported");
	}

	@Override
	/**
	 * Writes the int value, value, to the OutputStream.
	 */
	public void writeInt(int value) throws IOException {
		os.write(value >> 24);
		os.write(value >> 16);
		os.write(value >> 8);
		os.write(value);
	}

	@Override
	/**
	 * Writes the long value, value, to the OutputStream.
	 */
	public void writeLong(long value) throws IOException {
		os.write((int) (value >> 56));
		os.write((int) (value >> 48));
		os.write((int) (value >> 40));
		os.write((int) (value >> 32));
		os.write((int) (value >> 24));
		os.write((int) (value >> 16));
		os.write((int) (value >> 8));
		os.write((int) value);
	}

	@Override
	/**
	 * Writes the short value, value, to the OutputStream.
	 */
	public void writeShort(int value) throws IOException {
		os.write(value >> 8);
		os.write(value);
	}

	@Override
	public void writeUTF(String s) throws IOException {
		throw new IOException("Not supported");
	}

}
