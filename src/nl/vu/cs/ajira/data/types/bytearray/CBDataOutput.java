package nl.vu.cs.ajira.data.types.bytearray;

import java.io.IOException;

public class CBDataOutput extends BDataOutput {

	public CBDataOutput(ByteArray cb) {
		super(cb);
	}

	@Override
	public void write(int b) throws IOException {
		if (cb.end >= cb.buffer.length) {
			cb.end = 0;
		}
		super.write(b);
	}

	@Override
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
