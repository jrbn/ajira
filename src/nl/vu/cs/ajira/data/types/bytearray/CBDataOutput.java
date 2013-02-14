package nl.vu.cs.ajira.data.types.bytearray;

import java.io.IOException;

public class CBDataOutput extends BDataOutput {

	/**
	 * Creates a new CBDataOutput an sets the fields of the extended class.
	 * @param cb is the ByteArray of the extended class
	 * @param grow is the boolean value of the extended class
	 */
	public CBDataOutput(ByteArray cb, boolean grow) {
		super(cb, grow);
	}

	@Override
	/**
	 * If it is possible to add elements to the buffer then:
	 * 		If the end exceeds the length of the buffer then the end is reset.
	 * 		The method adds b at the end of the buffer.
	 */
	public void write(int b) throws IOException {
		if (grow && !cb.grow(1)) {
			throw new IOException("Not enough space");
		}

		if (cb.end >= cb.buffer.length) {
			cb.end = 0;
		}
		cb.buffer[cb.end++] = (byte) b;
	}

	@Override
	/**
	 * If it is possible to add elements to the buffer then:
	 * 		If the length is greater than the remaining bytes from the end
	 * 		of the buffer then are copied cb.buffer.length - cb.end bytes from 
	 * 		buffer2 at the end of the buffer and the remaining bytes from buffer2
	 * 		at the beginning of the buffer.
	 * 		If there are length bytes left in the buffer then length bytes are
	 * 		copied from buffer2 at the end of the buffer.
	 */
	public void write(byte[] buffer2, int offset, int length)
			throws IOException {
		if (grow && !cb.grow(length)) {
			throw new IOException("Not enough space");
		}

		if (length > cb.buffer.length - cb.end) {
			System.arraycopy(buffer2, offset, cb.buffer, cb.end,
					cb.buffer.length - cb.end);
			System.arraycopy(buffer2, offset + cb.buffer.length - cb.end,
					cb.buffer, 0, length - cb.buffer.length + cb.end);
			cb.end = length - cb.buffer.length + cb.end;
		} else {
			System.arraycopy(buffer2, offset, cb.buffer, cb.end, length);
			cb.end += length;
		}
	}

	@Override
	/**
	 * If it is possible to add elements to the buffer then:
	 * 		If there are 4 bytes at the end of the buffer then it writes 
	 * 		the int value. 
	 * 		If there are not 4 bytes at the end of the buffer then the 
	 * 		ending position of the buffer is reset.
	 */
	public void writeInt(int value) throws IOException {
		if (grow && !cb.grow(4)) {
			throw new IOException("Not enough space");
		}

		if (cb.end + 4 <= cb.buffer.length) {
			int end = cb.end;
			cb.buffer[end] = (byte) (value >> 24);
			cb.buffer[end + 1] = (byte) (value >> 16);
			cb.buffer[end + 2] = (byte) (value >> 8);
			cb.buffer[end + 3] = (byte) (value);
			cb.end = end + 4;
		} else {
			for (int i = 3; i >= 0; i--) {
				if (cb.end >= cb.buffer.length) {
					cb.end = 0;
				}
				cb.buffer[cb.end++] = (byte) (value >>> i * 8);
			}
		}
	}

	@Override
	/**
	 * If it is possible to add elements to the buffer then:
	 * 		If there are 2 bytes at the end of the buffer then it writes 
	 * 		the short value. 
	 * 		If there are not 2 bytes at the end of the buffer then the 
	 * 		ending position of the buffer is reset.
	 */
	public void writeShort(int value) throws IOException {
		if (grow && !cb.grow(2)) {
			throw new IOException("Not enough space");
		}

		if (cb.end + 2 <= cb.buffer.length) {
			int end = cb.end;
			cb.buffer[end] = (byte) (value >> 8);
			cb.buffer[end + 1] = (byte) (value);
			cb.end = end + 2;
		} else {
			if (cb.end >= cb.buffer.length) {
				cb.end = 0;
			}
			cb.buffer[cb.end++] = (byte) (value >>> 8);
			if (cb.end >= cb.buffer.length) {
				cb.end = 0;
			}
			cb.buffer[cb.end++] = (byte) value;
		}
	}

	@Override
	/**
	 * If it is possible to add elements to the buffer then:
	 * 		If there are 8 bytes at the end of the buffer then it writes 
	 * 		the long value. 
	 * 		If there are not 8 bytes at the end of the buffer then the 
	 * 		ending position of the buffer is reset.
	 */
	public void writeLong(long value) throws IOException {
		if (grow && !cb.grow(8)) {
			throw new IOException("Not enough space");
		}

		if (cb.end + 8 <= cb.buffer.length) {
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
		} else {
			for (int i = 7; i >= 0; i--) {
				if (cb.end >= cb.buffer.length) {
					cb.end = 0;
				}
				cb.buffer[cb.end++] = (byte) (value >>> i * 8);
			}
		}
	}

	@Override
	/**
	 * If it is possible to add elements to the buffer then:
	 * 		Skips bytes from the end of the buffer if this is possible or
	 * 		it skips bytes from the end and from the beginning of the buffer. 
	 */
	public void skipBytes(int bytes) throws IOException {
		if (grow && !cb.grow(bytes)) {
			throw new IOException("Not enough space");
		}

		if (cb.end + bytes < cb.buffer.length) {
			cb.end += bytes;
		} else {
			cb.end = bytes - (cb.buffer.length - cb.end);
		}
	}
}
