package nl.vu.cs.ajira.data.types.bytearray;

import java.io.DataOutput;
import java.io.IOException;

public class BDataOutput implements DataOutput {

	public ByteArray cb;
	protected boolean grow;
	
	/**
	 * Creates a new BDataOutput an sets the fields of the class.
	 * @param cb is the ByteArray of the object.
	 * @param grow is the new value of the field grow
	 */
	public BDataOutput(ByteArray cb, boolean grow) {
		this.cb = cb;
		this.grow = grow;
	}

	/**
	 * It constructs a new BDataOutput and sets cb's buffer to 
	 * the parameter and sets the grow to false.
	 * @param buffer is the new buffer of cb
	 */
	public BDataOutput(byte[] buffer) {
		cb = new ByteArray();
		cb.buffer = buffer;
		grow = false;
	}

	/**
	 * Sets the end of cb's buffer
	 * @param bufferSize is the new end of cb's buffer
	 */
	public void setCurrentPosition(int bufferSize) {
		cb.end = bufferSize;
	}
	
	@Override
	/**
	 * Adds b at the end of the ByteArray's buffer if there is enough space.
	 */
	public void write(int b) throws IOException {
		if (grow && !cb.grow(1)) {
			throw new IOException("Not enough space");
		}
		cb.buffer[cb.end++] = (byte) b;
	}

	@Override
	/**
	 * Copies array b at the end of cb's buffer if there is enough space.
	 */
	public void write(byte[] b) throws IOException {
		if (grow && !cb.grow(b.length)) {
			throw new IOException("Not enough space");
		}
		write(b, 0, b.length);
	}

	@Override
	/**
	 * Copies len bytes from the offset off of the array b
	 * at the end of cb's buffer if there is enough space.
	 */
	public void write(byte[] b, int off, int len) throws IOException {
		if (grow && !cb.grow(len)) {
			throw new IOException("Not enough space");
		}
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
	 * Writes the byte corresponding to the int value v 
	 * if there is enough space.
	 */
	public void writeByte(int v) throws IOException {
		if (grow && !cb.grow(1)) {
			throw new IOException("Not enough space");
		}
		write(v);
	}

	@Override
	public void writeBytes(String s) {
		throw new UnsupportedOperationException("Not supported");
	}

	@Override
	public void writeChar(int v) {
		throw new UnsupportedOperationException("Not supported");
	}

	@Override
	public void writeChars(String s) {
		throw new UnsupportedOperationException("Not supported");
	}

	@Override
	public void writeDouble(double v) {
		throw new UnsupportedOperationException("Not supported");
	}

	@Override
	public void writeFloat(float v) {
		throw new UnsupportedOperationException("Not supported");
	}

	@Override
	/**
	 * Writes the int value at the end of cb's buffer 
	 * if there is enough space.
	 */
	public void writeInt(int value) throws IOException {
		if (grow && !cb.grow(4)) {
			throw new IOException("Not enough space");
		}
		int end = cb.end;
		cb.buffer[end] = (byte) (value >> 24);
		cb.buffer[end + 1] = (byte) (value >> 16);
		cb.buffer[end + 2] = (byte) (value >> 8);
		cb.buffer[end + 3] = (byte) (value);
		cb.end = end + 4;
	}

	@Override
	/**
	 * Writes the long value at the end of cb's buffer 
	 * if there is enough space.
	 */
	public void writeLong(long value) throws IOException {
		if (grow && !cb.grow(8)) {
			throw new IOException("Not enough space");
		}
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
	 * Writes the short value at the end of cb's buffer 
	 * if there is enough space.
	 */
	public void writeShort(int value) throws IOException {
		if (grow && !cb.grow(2)) {
			throw new IOException("Not enough space");
		}
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

	/**
	 * Skips the number of bytes passed through the parameter.
	 * @param bytes is the number of bytes that are skiped
	 * @throws IOException
	 */
	public void skipBytes(int bytes) throws IOException {
		if (grow && !cb.grow(bytes)) {
			throw new IOException("Not enough space");
		}
		cb.end += bytes;
	}
}

