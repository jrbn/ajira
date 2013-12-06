package nl.vu.cs.ajira.examples.aurora.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import nl.vu.cs.ajira.data.types.SimpleData;
import nl.vu.cs.ajira.data.types.TInt;
import nl.vu.cs.ajira.data.types.TLong;
import nl.vu.cs.ajira.data.types.TString;
import nl.vu.cs.ajira.examples.aurora.data.exceptions.UnsupportedOperatorException;
import nl.vu.cs.ajira.storage.Writable;

/**
 * A simple constraint on the value of an attribute's value.
 * 
 * @author Alessandro Margara
 */
public class Constraint implements Writable {

  public enum ConstraintType {
    NUMERIC, STRING
  }

  private String name;
  private Op op;
  private ConstraintType type;
  private long numericVal;
  private String stringValue;

  public Constraint(String name, Op op, long val) throws UnsupportedOperatorException {
    this.name = name;
    this.op = op;
    type = ConstraintType.NUMERIC;
    numericVal = val;
    stringValue = null;
    if (op != Op.EQ && op != Op.GT && op != Op.LT && op != Op.DF) {
      throw new UnsupportedOperatorException();
    }
  }

  public Constraint(String name, Op op, String val) throws UnsupportedOperatorException {
    this.name = name;
    this.op = op;
    type = ConstraintType.NUMERIC;
    numericVal = 0;
    stringValue = val;
    if (op != Op.EQ && op != Op.PF && op != Op.IN && op != Op.DF) {
      throw new UnsupportedOperatorException();
    }
  }

  Constraint(DataInput input) throws IOException {
    readFrom(input);
  }

  public boolean isSatisfiedBy(StreamTuple tuple) {
    SimpleData tupleSimpleDataValue = tuple.getValueFor(name);
    if (tupleSimpleDataValue == null) return false;
    switch (type) {
    case NUMERIC:
      long tupleNumVal = 0;
      if (tupleSimpleDataValue instanceof TInt) {
        tupleNumVal = ((TInt) tupleSimpleDataValue).getValue();
      } else if (tupleSimpleDataValue instanceof TLong) {
        tupleNumVal = ((TLong) tupleSimpleDataValue).getValue();
      } else {
        return false;
      }
      return (op == Op.EQ && tupleNumVal == numericVal) || (op == Op.GT && tupleNumVal > numericVal) || (op == Op.LT && tupleNumVal < numericVal) || (op == Op.DF && tupleNumVal != numericVal);
    case STRING:
      if (!(tupleSimpleDataValue instanceof TString)) return false;
      String tupleStringVal = ((TString) tupleSimpleDataValue).getValue();
      return (op == Op.EQ && tupleStringVal.equals(stringValue)) || (op == Op.PF && tupleStringVal.startsWith(stringValue)) || (op == Op.IN && tupleStringVal.contains(stringValue)) || (op == Op.DF && !tupleStringVal.equals(stringValue));
    }
    return false;
  }

  @Override
  public void readFrom(DataInput input) throws IOException {
    boolean isNumeric = input.readBoolean();
    name = input.readUTF();
    op = Op.values()[input.readInt()];
    if (isNumeric) {
      type = ConstraintType.NUMERIC;
      numericVal = input.readLong();
    } else {
      type = ConstraintType.STRING;
      stringValue = input.readUTF();
    }

  }

  @Override
  public void writeTo(DataOutput output) throws IOException {
    boolean isNumeric = (type == ConstraintType.NUMERIC);
    output.writeBoolean(isNumeric);
    output.writeUTF(name);
    output.writeInt(op.ordinal());
    if (isNumeric) {
      output.writeLong(numericVal);
    } else {
      output.writeUTF(stringValue);
    }
  }

  public String getName() {
    return name;
  }

  public Op getOp() {
    return op;
  }

  public ConstraintType getType() {
    return type;
  }

  public long getNumericVal() {
    return numericVal;
  }

  public String getStringValue() {
    return stringValue;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((name == null) ? 0 : name.hashCode());
    result = prime * result + (int) (numericVal ^ (numericVal >>> 32));
    result = prime * result + ((op == null) ? 0 : op.hashCode());
    result = prime * result + ((stringValue == null) ? 0 : stringValue.hashCode());
    result = prime * result + ((type == null) ? 0 : type.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (!(obj instanceof Constraint)) {
      return false;
    }
    Constraint other = (Constraint) obj;
    if (name == null) {
      if (other.name != null) {
        return false;
      }
    } else if (!name.equals(other.name)) {
      return false;
    }
    if (numericVal != other.numericVal) {
      return false;
    }
    if (op != other.op) {
      return false;
    }
    if (stringValue == null) {
      if (other.stringValue != null) {
        return false;
      }
    } else if (!stringValue.equals(other.stringValue)) {
      return false;
    }
    if (type != other.type) {
      return false;
    }
    return true;
  }

}
