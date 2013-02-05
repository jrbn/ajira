package nl.vu.cs.ajira.data.types.bytearray;

import java.io.IOException;

public class CBDataOutput extends BDataOutput {

	/**
	 * Creates a new CBDataOutput an sets the field of the extended class.
	 * @param cb is the ByteArray of the extended class
	 */
	public CBDataOutput(ByteArray cb) {
		super(cb);
	}

	@Override
	/**
	 * If the end exceeds the length of the buffer then the end is reset.
	 * The method adds b at the end of the buffer.
	 */
	public void write(int b) throws IOException {
		if (cb.end >= cb.buffer.length) {
			cb.end = 0;
		}
		super.write(b);
	}

	@Override
	/**
	 * If the length is greater than the remaining bytes from the end
	 * of the buffer then are copied cb.buffer.length - cb.end bytes from 
	 * buffer2 at the end of the buffer and the remanig bytes from buffer2
	 * at the begining of the buffer.
	 * If the length feets the size of the buffer then length bytes from
	 * buffer2 are copied at the end of the buffer.
	 */
	public void write(byte[] buffer2, int offset, int length)
			throws IOException {
		if (length > cb.buffer.length - cb.end) {
			System.arraycopy(buffer2, offset, cb.buffer, cb.end,
					cb.buffer.length - cb.end);
			System.arraycopy(buffer2, offset + cb.buffer.length - cb.end,
					cb.buffer, 0, length - cb.buffer.length + cb.end);
			cb.end = length - cb.buffer.length + cb.end;
		} else {
			super.write(buffer2, offset, length);
		}
	}

	@Override
	/**
	 * If there are 4 bytes at the end of the buffer then it writes 
	 * the int value. If there are not 4 bytes at the end of the 
	 * buffer then the ending position of the buffer is reset.
	 */
	public void writeInt(int value) throws IOException {
		if (cb.end + 4 <= cb.buffer.length) {
			super.writeInt(value);
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
	 * If there are 2 bytes at the end of the buffer then it writes 
	 * the short value. If there are not 2 bytes at the end of the 
	 * buffer then the ending position of the buffer is reset.
	 */
	public void writeShort(int value) throws IOException {
		if (cb.end + 2 <= cb.buffer.length) {
			super.writeShort(value);
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
	 * If there are 8 bytes at the end of the buffer then it writes 
	 * the long value. If there are not 8 bytes at the end of the 
	 * buffer then the ending position of the buffer is reset.
	 */
	public void writeLong(long value) throws IOException {
		if (cb.end + 8 <= cb.buffer.length) {
			super.writeLong(value);
		} else {
			for (int i = 7; i >= 0; i--) {
				if (cb.end >= cb.buffer.length) {
					cb.end = 0;
				}
				cb.buffer[cb.end++] = (byte) (value >>> i * 8);
			}
		}
	}

}
