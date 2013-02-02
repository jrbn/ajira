package nl.vu.cs.ajira.data.types.bytearray;

import java.io.DataInput;
import java.io.IOException;

public class BDataInput implements DataInput {

	protected ByteArray cb;

	public BDataInput() {
		this.cb = new ByteArray();
	}

	public BDataInput(ByteArray cb) {
		this.cb = cb;
	}

	public BDataInput(byte[] buffer) {
		this();
		this.cb.buffer = buffer;
	}

	public void setBuffer(byte[] b1) {
		cb.buffer = b1;
	}

	public void setCurrentPosition(byte[] b1, int s1) {
		cb.buffer = b1;
		cb.start = s1;
	}

	public void setCurrentPosition(int i) {
		cb.start = i;
	}

	@Override
	public boolean readBoolean() throws IOException {
		return readByte() == 1;
	}

	@Override
	public byte readByte() throws IOException {
		return cb.buffer[cb.start++];
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
		System.arraycopy(cb.buffer, cb.start, b, off, len);
		cb.start += len;
	}

	@Override
	public int readInt() throws IOException {
		int value = 0;
		int start = cb.start;
		value += cb.buffer[start] << 24;
		value += (cb.buffer[start + 1] & 0xFF) << 16;
		value += (cb.buffer[start + 2] & 0xFF) << 8;
		value += (cb.buffer[start + 3] & 0xFF);
		cb.start = start + 4;
		return value;
	}

	@Override
	public String readLine() throws IOException {
		throw new IOException("Not supported");
	}

	@Override
	public long readLong() throws IOException {
		long value = 0;
		int start = cb.start;
		value = (long) cb.buffer[start] << 56;
		value += ((long) cb.buffer[start + 1] & 0xFF) << 48;
		value += ((long) cb.buffer[start + 2] & 0xFF) << 40;
		value += ((long) cb.buffer[start + 3] & 0xFF) << 32;
		value += ((long) cb.buffer[start + 4] & 0xFF) << 24;
		value += (cb.buffer[start + 5] & 0xFF) << 16;
		value += (cb.buffer[start + 6] & 0xFF) << 8;
		value += (cb.buffer[start + 7] & 0xFF);
		cb.start = start + 8;
		return value;
	}

	@Override
	public short readShort() throws IOException {
		short value = 0;
		int start = cb.start;
		value += cb.buffer[start] << 8;
		value += (cb.buffer[start + 1] & 0xFF);
		cb.start = start + 2;
		return value;
	}

	@Override
	public String readUTF() throws IOException {
		int size = readInt();
		String v = new String(cb.buffer, cb.start, size);
		cb.start += size;
		return v;
	}

	@Override
	public int readUnsignedByte() throws IOException {
		return readByte() & 0xFF;
	}

	@Override
	public int readUnsignedShort() throws IOException {
		throw new IOException("Not supported");
	}

	@Override
	public int skipBytes(int n) throws IOException {
		if (cb.start + n < cb.buffer.length) {
			cb.start += n;
		} else {
			cb.start = n - (cb.buffer.length - cb.start);
		}
		return n;
	}
}
