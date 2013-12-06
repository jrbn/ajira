package nl.vu.cs.ajira.examples.aurora.actions.operators.helpers;

import java.util.ArrayList;
import java.util.List;

import nl.vu.cs.ajira.data.types.SimpleData;
import nl.vu.cs.ajira.data.types.TInt;
import nl.vu.cs.ajira.data.types.TLong;
import nl.vu.cs.ajira.data.types.TString;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.examples.aurora.data.Constraint;
import nl.vu.cs.ajira.examples.aurora.data.Filter;
import nl.vu.cs.ajira.examples.aurora.data.Op;
import nl.vu.cs.ajira.examples.aurora.data.StreamTuple;
import nl.vu.cs.ajira.examples.aurora.data.Constraint.ConstraintType;

public class FilterHelper {
  private final List<InnerConstraint> constraintsList;

  public FilterHelper(Filter filter, StreamTuple tuple) {
    constraintsList = new ArrayList<InnerConstraint>();
    for (Constraint c : filter) {
      InnerConstraint ic = new InnerConstraint(c, tuple);
      constraintsList.add(ic);
    }
  }

  public boolean isSatisfiedBy(Tuple tuple) {
    for (InnerConstraint c : constraintsList) {
      if (!c.isSatisfiedBy(tuple)) {
        return false;
      }
    }
    return true;
  }

  private class InnerConstraint {
    private final int position;
    private final Op op;
    private final ConstraintType type;
    private final long numericVal;
    private final String stringValue;

    InnerConstraint(Constraint constraint, StreamTuple tuple) {
      op = constraint.getOp();
      type = constraint.getType();
      numericVal = constraint.getNumericVal();
      stringValue = constraint.getStringValue();
      position = tuple.getPositionOfValueFor(constraint.getName());
    }

    boolean isSatisfiedBy(Tuple tuple) {
      if (position == -1) return false;
      SimpleData tupleSimpleDataValue = tuple.get(position);
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
  }

}
