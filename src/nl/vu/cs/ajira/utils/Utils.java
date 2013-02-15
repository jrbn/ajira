package nl.vu.cs.ajira.utils;

import ibis.ipl.ReadMessage;
import ibis.ipl.WriteMessage;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.NumberFormat;
import java.util.Comparator;

import nl.vu.cs.ajira.data.types.DataProvider;
import nl.vu.cs.ajira.data.types.SimpleData;
import nl.vu.cs.ajira.data.types.TInt;
import nl.vu.cs.ajira.data.types.TLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Utils {

	public static void writeObjectsOnSimpleDataArray(SimpleData[] array, int i,
			Object[] params, DataProvider dp) {
		for (int m = 0; m < params.length; ++m) {
			Object param = params[m];
			if (param instanceof Long) {
				TLong value = (TLong) dp.get(Consts.DATATYPE_TLONG);
				value.setValue((Long) param);
				array[i + m] = value;
			}

			if (param instanceof Integer) {
				TInt value = (TInt) dp.get(Consts.DATATYPE_TINT);
				value.setValue((Integer) param);
				array[i + m] = value;
			}
		}
	}

	public static void writeObjectsOnMessage(WriteMessage message,
			Object[] params) throws IOException {
		for (int m = 0; m < params.length; ++m) {
			Object param = params[m];
			if (param instanceof Long) {
				message.writeByte((byte) 0);
				message.writeLong((Long) param);
			}

			if (param instanceof Integer) {
				message.writeByte((byte) 1);
				message.writeInt((Integer) param);
			}
		}
	}

	public static void readObjectsFromMessage(ReadMessage message,
			Object[] params) throws IOException {
		for (int m = 0; m < params.length; ++m) {
			byte flag = message.readByte();
			switch (flag) {
			case 0:
				params[m] = message.readLong();
				break;
			case 1:
				params[m] = message.readInt();
				break;
			}
		}
	}

	public static boolean createRecursevily(File dir) {
		if (!dir.getParentFile().exists()
				&& !createRecursevily(dir.getParentFile())) {
			return false;
		}

		return dir.mkdir();
	}

	public static class BytesComparator implements Comparator<byte[]> {

		@Override
		public int compare(byte[] o1, byte[] o2) {
			for (int i = 0, j = 0; i < o1.length && j < o2.length; i++, j++) {
				int a = (o1[i] & 0xff);
				int b = (o2[j] & 0xff);
				if (a != b) {
					return a - b;
				}
			}
			return o1.length - o2.length;
		}
	}

	static Logger log = LoggerFactory.getLogger(Utils.class);

	public static long decodeLong(byte[] value, int start) {
		int highword = ((value[start] & 0xFF) << 24)
				+ ((value[start + 1] & 0xFF) << 16)
				+ ((value[start + 2] & 0xFF) << 8) + (value[start + 3] & 0xFF);
		int lowword = ((value[start + 4] & 0xFF) << 24)
				+ ((value[start + 5] & 0xFF) << 16)
				+ ((value[start + 6] & 0xFF) << 8) + (value[start + 7] & 0xFF);
		return ((long) highword << 32) + (lowword & 0xFFFFFFFFL);
	}

	public static void encodeLong(byte[] value, int start, long number) {
		value[start++] = (byte) (number >> 56);
		value[start++] = (byte) (number >> 48);
		value[start++] = (byte) (number >> 40);
		value[start++] = (byte) (number >> 32);
		value[start++] = (byte) (number >> 24);
		value[start++] = (byte) (number >> 16);
		value[start++] = (byte) (number >> 8);
		value[start] = (byte) (number);
	}

	public static int decodeInt(byte[] value, int start) {
		return ((value[start] & 0xFF) << 24)
				+ ((value[start + 1] & 0xFF) << 16)
				+ ((value[start + 2] & 0xFF) << 8) + (value[start + 3] & 0xFF);
	}

	public static void encodeInt(byte[] value, int start, int number) {
		value[start++] = (byte) (number >> 24);
		value[start++] = (byte) (number >> 16);
		value[start++] = (byte) (number >> 8);
		value[start++] = (byte) (number);
	}

	public static long decodeLong(ByteBuffer buffer, int start) {
		return buffer.getLong(start);
	}

	public static void encodeLong(ByteBuffer buffer, int start, long number) {
		buffer.putLong(start, number);
	}

	public static int decodeInt(ByteBuffer buffer, int start) {
		return buffer.getInt(start);
	}

	public static void encodeInt(ByteBuffer buffer, int start, int number) {
		buffer.putInt(start, number);
	}

	public static int encodeInt2(byte[] value, int start, int number) {
		if (number < 0x40) { // Need at most 6 bytes to store it. I can use only
								// one byte
			value[start++] = (byte) (number & 0x3F);
		} else if (number < 0x4000) { // Need at most 2 bytes. I can use only
										// two bytes and set the first to 1
			value[start++] = (byte) (0x40 + (number >> 8 & 0x3F));
			value[start++] = (byte) (number & 0xFF);
		} else if (number < 0x400000) {
			value[start++] = (byte) (0x80 + (number >> 16 & 0x3F));
			value[start++] = (byte) (number >> 8 & 0xFF);
			value[start++] = (byte) (number & 0xFF);
		} else {
			value[start++] = (byte) (0xC0 + (number >> 24 & 0x3F));
			value[start++] = (byte) (number >> 16 & 0xFF);
			value[start++] = (byte) (number >> 8 & 0xFF);
			value[start++] = (byte) (number & 0xFF);
		}

		return start;
	}

	public static int encodeLong2(byte[] value, int start, long number) {

		// First write the counter
		int valueId = (int) (number >> 40);
		start = encodeInt2(value, start, valueId);

		// Clean the first part
		number &= 0xFFFFFFFFFFl;
		if (number < 0x20) { // Need only one byte
			value[start++] = (byte) (number & 0x1F);
		} else if (number < 0x2000) {
			value[start++] = (byte) (0x20 + (number >> 8 & 0x1F));
			value[start++] = (byte) (number & 0xFF);
		} else if (number < 0x200000) {
			value[start++] = (byte) (0x40 + (number >> 16 & 0x1F));
			value[start++] = (byte) (number >> 8 & 0xFF);
			value[start++] = (byte) (number & 0xFF);
		} else if (number < 0x200000) {
			value[start++] = (byte) (0x60 + (number >> 24 & 0x1F));
			value[start++] = (byte) (number >> 16 & 0xFF);
			value[start++] = (byte) (number >> 8 & 0xFF);
			value[start++] = (byte) (number & 0xFF);
		} else { // 5 bytes
			value[start++] = (byte) (0x80 + (number >> 32 & 0x1F));
			value[start++] = (byte) (number >> 24 & 0xFF);
			value[start++] = (byte) (number >> 16 & 0xFF);
			value[start++] = (byte) (number >> 8 & 0xFF);
			value[start++] = (byte) (number & 0xFF);
		}

		return start;
	}

	public static int decodeInt2(byte[] value, int[] position) {
		int start = position[0];
		int output = value[start] & 0x3F;
		int additionalBytes = (value[start++] & 0xFF) >> 6;
		switch (additionalBytes) {
		case 1:
			output = (output << 8) + (value[start++] & 0xFF);
			break;
		case 2:
			output = (output << 8) + (value[start++] & 0xFF);
			output = (output << 8) + (value[start++] & 0xFF);
			break;
		case 3:
			output = (output << 8) + (value[start++] & 0xFF);
			output = (output << 8) + (value[start++] & 0xFF);
			output = (output << 8) + (value[start++] & 0xFF);
			break;
		}

		position[1] = start;
		return output;
	}

	public static long decodeLong2(byte[] value, int[] position) {
		// Inlined decodeInt2 --Ceriel
		int start = position[0];
		int firstPart = value[start] & 0x3F;
		int additionalBytes = (value[start++] & 0xFF) >> 6;
		switch (additionalBytes) {
		case 1:
			firstPart = (firstPart << 8) + (value[start++] & 0xFF);
			break;
		case 2:
			firstPart = (firstPart << 8) + (value[start++] & 0xFF);
			firstPart = (firstPart << 8) + (value[start++] & 0xFF);
			break;
		case 3:
			firstPart = (firstPart << 8) + (value[start++] & 0xFF);
			firstPart = (firstPart << 8) + (value[start++] & 0xFF);
			firstPart = (firstPart << 8) + (value[start++] & 0xFF);
			break;
		}

		long output = (long) firstPart << 40;
		additionalBytes = (value[start] & 0xFF) >> 5;
		int b1 = value[start++] & 0x1F;
		switch (additionalBytes) {
		case 0:
			output += b1;
			break;
		case 1:
			b1 = (b1 << 8) + (value[start++] & 0xFF);
			output += b1;
			break;
		case 2:
			b1 = (b1 << 8) + (value[start++] & 0xFF);
			b1 = (b1 << 8) + (value[start++] & 0xFF);
			output += b1;
			break;
		case 3:
			b1 = (b1 << 8) + (value[start++] & 0xFF);
			b1 = (b1 << 8) + (value[start++] & 0xFF);
			b1 = (b1 << 8) + (value[start++] & 0xFF);
			output += b1;
			break;
		case 4:
			b1 = (b1 << 8) + (value[start++] & 0xFF);
			b1 = (b1 << 8) + (value[start++] & 0xFF);
			b1 = (b1 << 8) + (value[start++] & 0xFF);
			output += ((long) b1 << 8) + (value[start++] & 0xFF);
			break;
		}

		position[1] = start;
		return output;
	}

	public static String getDuration(long time) {
		NumberFormat f = NumberFormat.getInstance();
		f.setMinimumIntegerDigits(2);
		int milliseconds = (int) (time % 1000);
		time /= 1000;
		int seconds = (int) time % 60;
		time /= 60;
		int minutes = (int) time % 60;
		time /= 60;
		int hours = (int) time;
		String result = f.format(hours) + ":" + f.format(minutes) + ":"
				+ f.format(seconds);
		f.setMinimumIntegerDigits(3);
		result += ":" + f.format(milliseconds);
		return result;
	}
}
