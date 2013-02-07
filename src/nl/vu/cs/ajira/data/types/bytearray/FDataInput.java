package nl.vu.cs.ajira.data.types.bytearray;

import java.io.DataInput;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

public class FDataInput implements DataInput {

	InputStream is = null;

	public FDataInput(InputStream is) {
		this.is = is;
	}

	public void close() throws IOException {
		is.close();
	}

	@Override
	public boolean readBoolean() throws IOException {
		return is.read() == 1;
	}

	@Override
	public byte readByte() throws IOException {
		int b = is.read();
		if (b == -1) {
			throw new EOFException();
		}
		return (byte) b;
	}
	
	private int readb() throws IOException {
		int b = is.read();
		if (b == -1) {
			throw new EOFException();
		}
		return b;
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
	public void readFully(byte[] b) throws IOException {
		readFully(b, 0, b.length);
	}

	@Override
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
	public int readInt() throws IOException {
		int value = 0;
		value += readb() << 24;
		value += readb() << 16;
		value += readb() << 8;
		value += readb();
		return value;
	}

	@Override
	public String readLine() throws IOException {
		throw new IOException("Not supported");
	}

	@Override
	public long readLong() throws IOException {
		long value = 0;
		value = (long) readb() << 56;
		value += (long) readb() << 48;
		value += (long) readb() << 40;
		value += (long) readb() << 32;
		value += (long) readb() << 24;
		value += readb() << 16;
		value += readb() << 8;
		value += readb();
		return value;
	}

	@Override
	public short readShort() throws IOException {
		short value = 0;
		value += readByte() << 8;
		value += readb();
		return value;
	}

	@Override
	public String readUTF() throws IOException {
		throw new IOException("Not supported");
	}

	@Override
	public int readUnsignedByte() throws IOException {
		return readb();
	}

	@Override
	public int readUnsignedShort() throws IOException {
		int value = readb() << 8;
		value += readb();
		return value;
	}

	@Override
	public int skipBytes(int n) throws IOException {
		return (int) is.skip(n);
	}
}
