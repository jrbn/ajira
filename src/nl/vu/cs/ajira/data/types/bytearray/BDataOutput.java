package nl.vu.cs.ajira.data.types.bytearray;

import java.io.DataOutput;
import java.io.IOException;

public class BDataOutput implements DataOutput {

	public ByteArray cb;

	/**
	 * Creates a empty object.
	 */
	public BDataOutput() {
	}

	/**
	 * Creates a new BDataOutput an sets the field of the class.
	 * @param cb is the ByteArray of the object.
	 */
	public BDataOutput(ByteArray cb) {
		this.cb = cb;
	}

	/**
	 * It constructs a new BDataOutput and sets the buffer of the ByteArray.
	 * @param buffer is the new buffer of the ByteArray of the class
	 */
	public BDataOutput(byte[] buffer) {
		cb = new ByteArray();
		cb.buffer = buffer;
	}

	/**
	 * Sets the starting position and the buffer of the ByteArray.
	 * @param b1 is the new buffer of the ByteArray
	 * @param s1 is the new start position of the ByteArray
	 */
	public void setCurrentPosition(int bufferSize) {
		cb.end = bufferSize;
	}

	@Override
	/**
	 * Adds b at the end of the ByteArray's buffer.
	 */
	public void write(int b) throws IOException {
		cb.buffer[cb.end++] = (byte) b;
	}

	@Override
	/**
	 * Copies array b at the end of cb's buffer.
	 */
	public void write(byte[] b) throws IOException {
		write(b, 0, b.length);
	}

	@Override
	/**
	 * Copies len bytes from the offset off of the array b
	 * at the end of cb's buffer.
	 */
	public void write(byte[] b, int off, int len) throws IOException {
		System.arraycopy(b, off, cb.buffer, cb.end, len);
		cb.end += len;
	}

	@Override
	/**
	 * Writes the corresponding byte of the boolean value v.
	 */
	public void writeBoolean(boolean v) throws IOException {
		if (v)
			writeByte(1);
		else
			writeByte(0);
	}

	@Override
	/**
	 * Writes the byte corresponding to the int value v.
	 */
	public void writeByte(int v) throws IOException {
		write(v);
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
	 * Writes the int value at the end of cb's buffer.
	 */
	public void writeInt(int value) throws IOException {
		int end = cb.end;
		cb.buffer[end] = (byte) (value >> 24);
		cb.buffer[end + 1] = (byte) (value >> 16);
		cb.buffer[end + 2] = (byte) (value >> 8);
		cb.buffer[end + 3] = (byte) (value);
		cb.end = end + 4;
	}

	@Override
	/**
	 * Writes the long value at the end of cb's buffer.
	 */
	public void writeLong(long value) throws IOException {
		int end = cb.end;
		cb.buffer[end] = (byte) (value >> 56);
		cb.buffer[end + 1] = (byte) (value >> 48);
		cb.buffer[end + 2] = (byte) (value >> 40);
		cb.buffer[end + 3] = (byte) (value >> 32);
		cb.buffer[end + 4] = (byte) (value >> 24);
		cb.buffer[end + 5] = (byte) (value >> 16);
		cb.buffer[end + 6] = (byte) (value >> 8);
		cb.buffer[end + 7] = (byte) (value);
		cb.end = end + 8;
	}

	@Override
	/**
	 * Writes the short value at the end of cb's buffer.
	 */
	public void writeShort(int value) throws IOException {
		int end = cb.end;
		cb.buffer[end] = (byte) (value >> 8);
		cb.buffer[end + 1] = (byte) (value);
		cb.end = end + 2;
	}

	@Override
	/**
	 * Writes the lenght of the string and then the string 
	 * converted in bytes at the end of cb's buffer.
	 */
	public void writeUTF(String s) throws IOException {
		byte[] b = s.getBytes();
		writeInt(b.length);
		write(b);
	}
}