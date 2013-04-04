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

		public int compare(byte[] o1, byte[] o2, int maxBytes) {
			for (int i = 0, j = 0; i < maxBytes; i++, j++) {
				int a = (o1[i] & 0xff);
				int b = (o2[j] & 0xff);
				if (a != b) {
					return a - b;
				}
			}
			return 0;
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
	
	private static int numBytesPackedLong(long number) {
		long max = 32;
		if (number < 0) {
			throw new Error("Negative number in encode2Long2()" + number);
		}
		for (int i = 1; i <= 8; i++) {
			if (number < max) {
				return i;
			}
			max *= 256;
		}
		throw new Error("number too large for encodeLong2(): " + number);
	}
	
	// Order preserving packed encoding of long values.
	public static int encodePackedLong(byte[] value, int start, long number) {
		int nbytes = numBytesPackedLong(number);
		for (int i = nbytes-1; i > 0; i--) {
			value[start+i] = (byte) ((int)number & 255);
			number >>= 8;
		}
		
		value[start] = (byte) (((nbytes-1) << 5) + ((int) number & 31));
		return start + nbytes;
	}

	public static long decodePackedLong(byte[] value, int[] position) {
		int start = position[0];
		int nbytes = (value[start] & 255) >> 5;
		long retval = (value[start] & 31);
		for (int i = 1; i < nbytes; i++) {
			retval <<= 8;
			retval += (value[start+i] & 255);
		}
		position[0] = start + nbytes;
		return retval;
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
