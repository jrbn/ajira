package nl.vu.cs.ajira.data.types.bytearray;

import java.io.DataInput;
import java.io.IOException;

public class BDataInput implements DataInput {

	protected ByteArray cb;

	/**
	 * Creates a empty object.
	 */
	public BDataInput() {
		this.cb = new ByteArray();
	}
	
	/**
	 * Creates a new BDataInput an sets the field of the class.
	 * @param cb is the ByteArray of the object.
	 */
	public BDataInput(ByteArray cb) {
		this.cb = cb;
	}

	/**
	 * It constructs a new BDataInput and sets the buffer of the ByteArray.
	 * @param buffer is the new buffer of the ByteArray of the class
	 */
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
	
	/**
	 * Sets the start position of the ByteArray
	 * @param i is the new start position of the ByteArray
	 */
	public void setCurrentPosition(int i) {
		cb.start = i;
	}

	@Override
	/**
	 * Returns the first byte from the beginning of 
	 * the ByteArray's buffer. 
	 */
	public boolean readBoolean() throws IOException {
		return readByte() == 1;
	}

	@Override
	/**
	 * Returns the first byte from the beginning of 
	 * the ByteArray's buffer. 
	 */
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
	/**
	 *  Copies b in cb's buffer.
	 */
	public void readFully(byte[] b) throws IOException {
		readFully(b, 0, b.length);
	}

	@Override
	/**
	 * Copies len bytes from b's off position in cb's buffer.
	 */
	public void readFully(byte[] b, int off, int len) throws IOException {
		System.arraycopy(cb.buffer, cb.start, b, off, len);
		cb.start += len;
	}

	@Override
	/**
	 * Returns the first int value from the beginning of 
	 * the ByteArray's buffer
	 */
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
	/**
	 * Returns the first long value from the beginning of 
	 * the ByteArray's buffer
	 */
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
	/**
	 * Returns the first short value from the beginning of 
	 * the ByteArray's buffer
	 */
	public short readShort() throws IOException {
		short value = 0;
		int start = cb.start;
		value += cb.buffer[start] << 8;
		value += (cb.buffer[start + 1] & 0xFF);
		cb.start = start + 2;
		return value;
	}

	@Override
	/**
	 * Takes the first int from the beginning of 
	 * the ByteArray and then creates a new String 
	 * of the size just read. 
	 */
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
	/**
	 * Returns the first unsigned byte from the
	 * beginning of the ByteArray's buffer. 
	 */
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