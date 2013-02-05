package nl.vu.cs.ajira.data.types.bytearray;

import java.io.DataInput;
import java.io.IOException;
import java.io.InputStream;

public class FDataInput implements DataInput {

	InputStream is = null;

	/**
	 * Creates a new FDataInput and sets the field of the class.
	 * @param is the new InputStream of the class
	 */
	public FDataInput(InputStream is) {
		this.is = is;
	}

	/**
	 * Closes the InputStream of the class.
	 * @throws IOException
	 */
	public void close() throws IOException {
		is.close();
	}

	@Override
	/**
	 * Reads one boolean value from the InputStream.
	 */
	public boolean readBoolean() throws IOException {
		return is.read() == 1;
	}

	@Override
	/**
	 * Reads one byte value from the InputStream.
	 */
	public byte readByte() throws IOException {
		return (byte) is.read();
	}

	@Override
	public char readChar() throws IOException {
		throw new IOException("Not supported");
	}

	@Override
	public double readDouble() throws IOException {
		throw new IOException("Not supported");
	}

	@Override
	public float readFloat() throws IOException {
		throw new IOException("Not supported");
	}

	@Override
	/**
	 * Reads in b, b.length bytes from the InputStream.
	 */
	public void readFully(byte[] b) throws IOException {
		readFully(b, 0, b.length);
	}

	@Override
	/**
	 * Reads len bytes from the InputStream in b from the offset off.
	 */
	public void readFully(byte[] b, int off, int len) throws IOException {
                int remaining = len;
                while (remaining > 0) {
                    int rd = is.read(b, off, remaining);
                    if (rd < 0) {
                        throw new IOException("EOF encountered in readFully");
                    }
                    remaining -= rd;
                    off += rd;
                }
	}

	@Override
	/**
	 * Reads one int value from the InputStream and returns this value.
	 */
	public int readInt() throws IOException {
		int value = 0;
		value += (byte) is.read() << 24;
		value += is.read() << 16;
		value += is.read() << 8;
		value += is.read();
		return value;
	}

	@Override
	public String readLine() throws IOException {
		throw new IOException("Not supported");
	}

	@Override
	/**
	 * Reads one long value from the InputStream and returns this value.
	 */
	public long readLong() throws IOException {
		long value = 0;
		value = (long) is.read() << 56;
		value += (long) is.read() << 48;
		value += (long) is.read() << 40;
		value += (long) is.read() << 32;
		value += (long) is.read() << 24;
		value += is.read() << 16;
		value += is.read() << 8;
		value += is.read();
		return value;
	}

	@Override
	/**
	 * Reads one short value from the InputStream and returns this value.
	 */
	public short readShort() throws IOException {
		short value = 0;
		value += (byte) is.read() << 8;
		value += is.read();
		return value;
	}

	@Override
	public String readUTF() throws IOException {
		throw new IOException("Not supported");
	}

	@Override
	/**
	 * Reads one unsigned byte value from the InputStream and returns this value.
	 */
	public int readUnsignedByte() throws IOException {
		return is.read();
	}

	@Override
	public int readUnsignedShort() throws IOException {
		throw new IOException("Not supported");
	}

	@Override
	public int skipBytes(int n) throws IOException {
		throw new IOException("Not supported");
	}

}
