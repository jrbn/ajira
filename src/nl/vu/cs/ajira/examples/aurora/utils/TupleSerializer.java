package nl.vu.cs.ajira.examples.aurora.utils;

import nl.vu.cs.ajira.data.types.SimpleData;
import nl.vu.cs.ajira.data.types.TBoolean;
import nl.vu.cs.ajira.data.types.TByteArray;
import nl.vu.cs.ajira.data.types.TInt;
import nl.vu.cs.ajira.data.types.TLong;
import nl.vu.cs.ajira.data.types.TString;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.data.types.TupleFactory;
import nl.vu.cs.ajira.examples.aurora.common.Consts;
import nl.vu.cs.ajira.utils.Utils;

public class TupleSerializer {

  public static final byte INT = 0;
  public static final byte LONG = 1;
  public static final byte STRING = 2;
  public static final byte BOOLEAN = 3;

  private static final int getSize(Tuple tuple) {
    int count = 1; // One byte for the number of elements
    for (int i = 0; i < tuple.getNElements(); i++) {
      SimpleData val = tuple.get(i);
      if (val instanceof TString) {
        count += getSize((TString) val);
      } else if (val instanceof TInt) {
        count += getSize((TInt) val);
      } else if (val instanceof TLong) {
        count += getSize((TLong) val);
      } else if (val instanceof TBoolean) {
        count += getSize((TBoolean) val);
      }
    }
    return count;
  }

  private static final int getSize(int val) {
    return 5;
  }

  private static final int getSize(TInt val) {
    return getSize(val.getValue());
  }

  private static final int getSize(long val) {
    return 9;
  }

  private static final int getSize(TLong val) {
    return getSize(val.getValue());
  }

  private static final int getSize(String val) {
    return 2 + val.getBytes().length;
  }

  private static final int getSize(TString val) {
    return getSize(val.getValue());
  }

  private static final int getSize(boolean val) {
    return 2;
  }

  private static final int getSize(TBoolean val) {
    return getSize(val.getValue());
  }

  private static final TInt getInt(byte[] bytes, int start) {
    return new TInt(Utils.decodeInt(bytes, start));
  }

  private static final int encodeInt(byte[] bytes, int start, int value) {
    bytes[start] = INT;
    Utils.encodeInt(bytes, start + 1, value);
    return start + 5;
  }

  private static final int encodeInt(byte[] bytes, int start, TInt value) {
    return encodeInt(bytes, start, value.getValue());
  }

  private static final TLong getLong(byte[] bytes, int start) {
    return new TLong(Utils.decodeLong(bytes, start));
  }

  private static final int encodeLong(byte[] bytes, int start, long value) {
    bytes[start] = LONG;
    Utils.encodeLong(bytes, start + 1, value);
    return start + 9;
  }

  private static final int encodeLong(byte[] bytes, int start, TLong value) {
    return encodeLong(bytes, start, value.getValue());
  }

  private static final TBoolean getBoolean(byte[] bytes, int start) {
    return new TBoolean(bytes[start] == 0);
  }

  private static final int encodeBoolean(byte[] bytes, int start, boolean value) {
    bytes[start] = BOOLEAN;
    bytes[start + 1] = (byte) (value ? 1 : 0);
    return start + 2;
  }

  private static final int encodeBoolean(byte[] bytes, int start, TBoolean value) {
    return encodeBoolean(bytes, start, value.getValue());
  }

  private static final TString getString(byte[] bytes, int start, int size) {
    return new TString(new String(bytes, start, size));
  }

  private static final int encodeString(byte[] bytes, int start, String value) {
    int stringLen = (byte) value.length();
    bytes[start] = STRING;
    bytes[start + 1] = (byte) stringLen;
    byte[] stringByte = value.getBytes();
    for (int i = 0; i < stringLen; i++) {
      bytes[start + i + 2] = stringByte[i];
    }
    return start + 2 + stringLen;
  }

  private static final int encodeString(byte[] bytes, int start, TString value) {
    return encodeString(bytes, start, value.getValue());
  }

  public static final byte[] encodeTuple(Tuple tuple) {
    int size = getSize(tuple);
    int numElements = tuple.getNElements();
    int start = 0;
    byte[] bytes = new byte[size];
    bytes[start++] = (byte) numElements;
    for (int i = 0; i < numElements; i++) {
      SimpleData val = tuple.get(i);
      if (val instanceof TInt) {
        start = encodeInt(bytes, start, (TInt) val);
      } else if (val instanceof TLong) {
        start = encodeLong(bytes, start, (TLong) val);
      } else if (val instanceof TString) {
        start = encodeString(bytes, start, (TString) val);
      } else if (val instanceof TBoolean) {
        start = encodeBoolean(bytes, start, (TBoolean) val);
      }
    }
    return bytes;
  }

  public static final byte[] encodeTuple(Tuple tuple, int channelId) {
    int size = getSize(tuple) + getSize(Consts.CHANNEL_ATTRIBUTE) + getSize(channelId);
    int numElements = tuple.getNElements();
    int start = 0;
    byte[] bytes = new byte[size];
    bytes[start++] = (byte) (numElements + 2);
    start = encodeString(bytes, start, Consts.CHANNEL_ATTRIBUTE);
    start = encodeInt(bytes, start, channelId);
    for (int i = 0; i < numElements; i++) {
      SimpleData val = tuple.get(i);
      if (val instanceof TInt) {
        start = encodeInt(bytes, start, (TInt) val);
      } else if (val instanceof TLong) {
        start = encodeLong(bytes, start, (TLong) val);
      } else if (val instanceof TString) {
        start = encodeString(bytes, start, (TString) val);
      } else if (val instanceof TBoolean) {
        start = encodeBoolean(bytes, start, (TBoolean) val);
      }
    }
    return bytes;
  }

  public static final Tuple getTuple(byte[] bytes) {
    int pos = 0;
    int numElements = bytes[pos++];
    SimpleData[] data = new SimpleData[numElements];
    for (int i = 0; i < numElements; i++) {
      byte type = bytes[pos++];
      switch (type) {
      case INT:
        data[i] = getInt(bytes, pos);
        pos += 4;
        break;
      case LONG:
        data[i] = getLong(bytes, pos);
        pos += 8;
        break;
      case STRING:
        int stringLen = bytes[pos++];
        data[i] = getString(bytes, pos, stringLen);
        pos += stringLen;
        break;
      case BOOLEAN:
        data[i] = getBoolean(bytes, pos);
        pos++;
        break;
      }
    }
    return TupleFactory.newTuple(data);
  }

  public static final Tuple getTuple(TByteArray bytes) {
    return getTuple(bytes.getArray());
  }
}
