package nl.vu.cs.ajira.data.types.bytearray;

import java.io.IOException;

public class CBDataInput extends BDataInput {

	@Override
	public byte readByte() throws IOException {
		if (cb.start >= cb.buffer.length)
			cb.start = 0;
		return super.readByte();
	}

	@Override
	public void readFully(byte[] b, int off, int len) throws IOException {
		if (len > cb.buffer.length - cb.start) {
			System.arraycopy(cb.buffer, cb.start, b, off, cb.buffer.length
					- cb.start);
			System.arraycopy(cb.buffer, 0, b,
					off + cb.buffer.length - cb.start, len - cb.buffer.length
							+ cb.start);
			cb.start = len - cb.buffer.length + cb.start;
		} else {
			super.readFully(b, off, len);
		}
	}

	@Override
	public int readInt() throws IOException {
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
	public short readShort() throws IOException {
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
	public long readLong() throws IOException {

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

	public CBDataInput(ByteArray cb) {
		super(cb);
	}
}
