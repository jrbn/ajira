package nl.vu.cs.ajira.net;

import ibis.ipl.ReadMessage;

import java.io.DataInput;
import java.io.IOException;

class ReadMessageWrapper implements DataInput {

	ReadMessage message;

	public ReadMessageWrapper(ReadMessage message) {
		this.message = message;
	}

	@Override
	public boolean readBoolean() throws IOException {
		return message.readBoolean();
	}

	@Override
	public byte readByte() throws IOException {
		return message.readByte();
	}

	@Override
	public char readChar() throws IOException {
		return message.readChar();
	}

	@Override
	public double readDouble() throws IOException {
		return message.readDouble();
	}

	@Override
	public float readFloat() throws IOException {
		return message.readFloat();
	}

	@Override
	public void readFully(byte[] b) throws IOException {
		message.readArray(b);
	}

	@Override
	public void readFully(byte[] b, int off, int len) throws IOException {
		message.readArray(b, off, len);
	}

	@Override
	public int readInt() throws IOException {
		return message.readInt();
	}

	@Override
	public String readLine() throws IOException {
		return message.readString();
	}

	@Override
	public long readLong() throws IOException {
		return message.readLong();
	}

	@Override
	public short readShort() throws IOException {
		return message.readShort();
	}

	@Override
	public String readUTF() throws IOException {
		return message.readString();
	}

	@Override
	public int readUnsignedByte() throws IOException {
		return (message.readByte() & 0xFF);
	}

	@Override
	public int readUnsignedShort() throws IOException {
		return message.readShort() & 0xFFFF;
	}

	@Override
	public int skipBytes(int n) throws IOException {
		for (int i = 0; i < n; ++i) {
			message.readByte();
		}
		return n;
	}

	public void closeMessage() throws IOException {
		message.finish();
	}
}
