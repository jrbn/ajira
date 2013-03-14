package nl.vu.cs.ajira.data.types.bytearray;

public class CBDataInput extends BDataInput {

	@Override
	/**
	 * If the starting position of the buffer is greater than it's 
	 * length then the staring position is reset to 0. The method 
	 * returns the first byte from the beginning of the buffer. 
	 */
	public byte readByte() {
		if (cb.start >= cb.buffer.length)
			cb.start = 0;
		return super.readByte();
	}

	@Override
	/**
	 * If len is greater than the number of elements that are at the 
	 * end of the buffer then it copies in b the elements at the end 
	 * of the buffer and resets the starting position of the buffer
	 * and copies the remaining elements from the beginning. If there
	 * are len elements at the end of the buffer then it calls the 
	 * readFully method from the extended class. 
	 */
	public void readFully(byte[] b, int off, int len) {
		if (len > cb.buffer.length - cb.start) {
			int len1 = cb.buffer.length - cb.start;
			System.arraycopy(cb.buffer, cb.start, b, off, len1);
			System.arraycopy(cb.buffer, 0, b,
					off + len1, len - len1);
			cb.start = len - len1;
		} else {
			super.readFully(b, off, len);
		}
	}

	@Override
	/**
	 * If there are 4 bytes at the end of the buffer then it reads 
	 * the int value. If there are not 4 bytes at the end of the 
	 * buffer then the starting position of the buffer is reset. 
	 */
	public int readInt() {
		if (cb.start + 4 <= cb.buffer.length)
			return super.readInt();
		else {
			int value = 0;
			for (int i = 3; i >= 0; i--) {
				if (cb.start >= cb.buffer.length) {
					cb.start = 0;
				}
				if (i == 3) {
					value += (cb.buffer[cb.start++]) << i * 8;
				} else {
					value += (cb.buffer[cb.start++] & 0xFF) << i * 8;
				}
			}
			return value;
		}
	}
	
	@Override
	/**
	 * If there are 2 bytes at the end of the buffer then it reads 
	 * the short value. If there are not 2 bytes at the end of the 
	 * buffer then the starting position of the buffer is reset.
	 */
	public short readShort() {
		if (cb.start + 2 <= cb.buffer.length)
			return super.readShort();
		else {
			if (cb.start >= cb.buffer.length) {
				cb.start = 0;
			}
			short value = (short)((cb.buffer[cb.start++]) << 8);
			if (cb.start >= cb.buffer.length) {
				cb.start = 0;
			}		
			value += (cb.buffer[cb.start++] & 0xFF);
			return value;
		}
	}

	@Override
	/**
	 * If there are 8 bytes at the end of the buffer then it reads 
	 * the long value. If there are not 8 bytes at the end of the 
	 * buffer then the starting position of the buffer is reset.
	 */
	public long readLong() {
		if (cb.start + 8 <= cb.buffer.length) {
			return super.readLong();

		} else {
			long value = 0;
			for (int i = 7; i >= 0; i--) {
				if (cb.start >= cb.buffer.length) {
					cb.start = 0;
				}
				if (i == 7) {
					value += (long) cb.buffer[cb.start++] << i * 8;
				} else {
					value += ((long) cb.buffer[cb.start++] & 0xFF) << i * 8;
				}
			}
			return value;
		}
	}

	/**
	 * Creates a new CBDataInput an sets the field of the extended class.
	 * @param cb is the ByteArray of the extended class
	 */
	public CBDataInput(ByteArray cb) {
		super(cb);
	}
}
