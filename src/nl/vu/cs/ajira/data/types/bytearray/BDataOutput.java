package nl.vu.cs.ajira.data.types.bytearray;

import java.io.DataOutput;
import java.io.IOException;

public class BDataOutput implements DataOutput {

	public ByteArray cb;
	protected boolean grow;

	public BDataOutput(ByteArray cb, boolean grow) {
		this.cb = cb;
		this.grow = grow;
	}

	public BDataOutput(byte[] buffer) {
		cb = new ByteArray();
		cb.buffer = buffer;
		grow = false;
	}

	public void setCurrentPosition(int bufferSize) {
		cb.end = bufferSize;
	}

	@Override
	public void write(int b) throws IOException {
		if (grow && !cb.grow(1)) {
			throw new IOException("Not enough space");
		}
		cb.buffer[cb.end++] = (byte) b;
	}

	@Override
	public void write(byte[] b) throws IOException {
		if (grow && !cb.grow(b.length)) {
			throw new IOException("Not enough space");
		}
		write(b, 0, b.length);
	}

	@Override
	public void write(byte[] b, int off, int len) throws IOException {
		if (grow && !cb.grow(len)) {
			throw new IOException("Not enough space");
		}
		System.arraycopy(b, off, cb.buffer, cb.end, len);
		cb.end += len;
	}

	@Override
	public void writeBoolean(boolean v) throws IOException {
		if (v)
			writeByte(1);
		else
			writeByte(0);
	}

	@Override
	public void writeByte(int v) throws IOException {
		if (grow && !cb.grow(1)) {
			throw new IOException("Not enough space");
		}
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
	public void writeUTF(String s) throws IOException {
		byte[] b = s.getBytes();
		writeInt(b.length);
		write(b);
	}
}