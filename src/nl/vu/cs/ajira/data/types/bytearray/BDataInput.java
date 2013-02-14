package nl.vu.cs.ajira.data.types.bytearray;

import java.io.DataInput;

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

	/**
	 * Sets the buffer of cb.
	 * @param b1 is the new buffer of cb
	 */
	public void setBuffer(byte[] b1) {
		cb.buffer = b1;
	}

	/**
	 * Sets the buffer and the starting position of cb.
	 * @param b1 is the new buffer of cb
	 * @param s1 is the new start position of cb
	 */
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
	public boolean readBoolean() {
		return readByte() == 1;
	}

	@Override
	/**
	 * Returns the first byte from the beginning of 
	 * the ByteArray's buffer. 
	 */
	public byte readByte() {
		return cb.buffer[cb.start++];
	}

	@Override
	public char readChar() {
		throw new UnsupportedOperationException("Not supported");
	}

	@Override
	public double readDouble() {
		throw new UnsupportedOperationException("Not supported");
	}

	@Override
	public float readFloat() {
		throw new UnsupportedOperationException("Not supported");
	}

	@Override
	/**
	 *  Copies b in cb's buffer.
	 */
	public void readFully(byte[] b) {
		readFully(b, 0, b.length);
	}

	@Override
	/**
	 * Copies len bytes from b's off position in cb's buffer.
	 */
	public void readFully(byte[] b, int off, int len) {
		System.arraycopy(cb.buffer, cb.start, b, off, len);
		cb.start += len;
	}

	@Override
	/**
	 * Returns the first int value from the beginning of 
	 * the ByteArray's buffer
	 */
	public int readInt() {
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
	public String readLine() {
		throw new UnsupportedOperationException("Not supported");
	}

	@Override
	/**
	 * Returns the first long value from the beginning of 
	 * the ByteArray's buffer
	 */
	public long readLong() {
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
	public short readShort() {
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
	public String readUTF() {
		int size = readInt();
		String v = new String(cb.buffer, cb.start, size);
		cb.start += size;
		return v;
	}

	@Override
	/**
	 * Returns the first unsigned byte from the
	 * beginning of the ByteArray's buffer. 
	 */
	public int readUnsignedByte() {
		return readByte() & 0xFF;
	}

	@Override
	public int readUnsignedShort()  {
		throw new UnsupportedOperationException("Not supported");
	}

	@Override
	/**
	 * Skips the number of bytes passed through the parameter.
	 */
	public int skipBytes(int n) {
		if (cb.start + n < cb.buffer.length) {
			cb.start += n;
		} else {
			cb.start = n - (cb.buffer.length - cb.start);
		}
		return n;
	}
}
