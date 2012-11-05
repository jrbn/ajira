package arch.net;

import ibis.ipl.WriteMessage;

import java.io.DataOutput;
import java.io.IOException;

public class WriteMessageWrapper implements DataOutput {

	WriteMessage message;

	public WriteMessageWrapper(WriteMessage message) {
		this.message = message;
	}

	@Override
	public void write(int b) throws IOException {
		message.writeByte((byte) b);
	}

	@Override
	public void write(byte[] b) throws IOException {
		message.writeArray(b);
	}

	@Override
	public void write(byte[] b, int off, int len) throws IOException {
		message.writeArray(b, off, len);
	}

	@Override
	public void writeBoolean(boolean v) throws IOException {
		message.writeBoolean(v);
	}

	@Override
	public void writeByte(int v) throws IOException {
		message.writeByte((byte) v);
	}

	@Override
	public void writeBytes(String s) throws IOException {
		byte[] l = s.getBytes();
		message.writeInt(l.length);
		message.writeArray(l);
	}

	@Override
	public void writeChar(int v) throws IOException {
		message.writeChar((char)v);
	}

	@Override
	public void writeChars(String s) throws IOException {
		char[] l = s.toCharArray();
		message.writeInt(l.length);
		message.writeArray(l);
	}

	@Override
	public void writeDouble(double v) throws IOException {
		message.writeDouble(v);
	}

	@Override
	public void writeFloat(float v) throws IOException {
		message.writeFloat(v);
	}

	@Override
	public void writeInt(int v) throws IOException {
		message.writeInt(v);
	}

	@Override
	public void writeLong(long v) throws IOException {
		message.writeLong(v);
	}

	@Override
	public void writeShort(int v) throws IOException {
		message.writeShort((short) v);
	}

	@Override
	public void writeUTF(String s) throws IOException {
		message.writeString(s);
	}

}
