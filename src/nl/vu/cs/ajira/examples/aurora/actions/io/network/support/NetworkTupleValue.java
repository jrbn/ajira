package nl.vu.cs.ajira.examples.aurora.actions.io.network.support;

import java.io.Serializable;

public class NetworkTupleValue implements Serializable {
  private static final long serialVersionUID = -7290041559929300708L;

  private final String stringVal;
  private final int intVal;
  private final long longVal;
  private final boolean booleanVal;
  private final NetworkValueType type;

  public NetworkTupleValue(String value) {
    type = NetworkValueType.STRING;
    stringVal = value;
    intVal = 0;
    longVal = 0;
    booleanVal = false;
  }

  public NetworkTupleValue(int value) {
    type = NetworkValueType.INT;
    stringVal = null;
    intVal = value;
    longVal = 0;
    booleanVal = false;
  }

  public NetworkTupleValue(long value) {
    type = NetworkValueType.LONG;
    stringVal = null;
    intVal = 0;
    longVal = value;
    booleanVal = false;
  }

  public NetworkTupleValue(boolean value) {
    type = NetworkValueType.BOOLEAN;
    stringVal = null;
    intVal = 0;
    longVal = 0;
    booleanVal = value;
  }

  public String getStringVal() {
    return stringVal;
  }

  public int getIntVal() {
    return intVal;
  }

  public long getLongVal() {
    return longVal;
  }

  public boolean getBooleanVal() {
    return booleanVal;
  }

  public NetworkValueType getType() {
    return type;
  }

}
