package nl.vu.cs.ajira.data.types.bytearray;

import java.io.DataOutput;

public class BDataOutput implements DataOutput {

	public ByteArray cb;

	public BDataOutput() {
	}

	public BDataOutput(ByteArray cb) {
		this.cb = cb;
	}

	public BDataOutput(byte[] buffer) {
		cb = new ByteArray();
		cb.buffer = buffer;
	}

	public void setCurrentPosition(int bufferSize) {
		cb.end = bufferSize;
	}

	@Override
	public void write(int b) {
		cb.buffer[cb.end++] = (byte) b;
	}

	@Override
	public void write(byte[] b) {
		write(b, 0, b.length);
	}

	@Override
	public void write(byte[] b, int off, int len) {
		System.arraycopy(b, off, cb.buffer, cb.end, len);
		cb.end += len;
	}

	@Override
	public void writeBoolean(boolean v) {
		if (v)
			writeByte(1);
		else
			writeByte(0);
	}

	@Override
	public void writeByte(int v) {
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
	public void writeInt(int value) {
		int end = cb.end;
		cb.buffer[end] = (byte) (value >> 24);
		cb.buffer[end + 1] = (byte) (value >> 16);
		cb.buffer[end + 2] = (byte) (value >> 8);
		cb.buffer[end + 3] = (byte) (value);
		cb.end = end + 4;
	}

	@Override
	public void writeLong(long value) {
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
	public void writeShort(int value) {
		int end = cb.end;
		cb.buffer[end] = (byte) (value >> 8);
		cb.buffer[end + 1] = (byte) (value);
		cb.end = end + 2;
	}

	@Override
	public void writeUTF(String s) {
		byte[] b = s.getBytes();
		writeInt(b.length);
		write(b);
	}
}