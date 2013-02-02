package nl.vu.cs.ajira.data.types.bytearray;

import java.io.IOException;

public class CBDataOutput extends BDataOutput {

	public CBDataOutput(ByteArray cb, boolean grow) {
		super(cb, grow);
	}

	@Override
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
