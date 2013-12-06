package nl.vu.cs.ajira.examples.aurora.actions.operators.helpers;

import java.util.ArrayList;
import java.util.List;

import nl.vu.cs.ajira.data.types.SimpleData;
import nl.vu.cs.ajira.data.types.TBoolean;
import nl.vu.cs.ajira.data.types.TInt;
import nl.vu.cs.ajira.data.types.TLong;
import nl.vu.cs.ajira.data.types.TString;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.examples.aurora.data.Ordering;

public class SortHelper {
  private final List<Tuple> list;
  private final int slack;
  private final int position;
  private final Ordering ordering;

  public SortHelper(Tuple tuple, String attribute, int slack, Ordering ordering) {
    this.slack = slack;
    this.ordering = ordering;
    position = getPositionOfValueFor(tuple, attribute);
    list = new ArrayList<Tuple>();
  }

  /**
   * If the stack is full, removes one element before pushing the tuple, and returns it, otherwise returns null.
   */
  public Tuple push(Tuple tuple) {
    Tuple removedTuple = null;
    if (list.size() > slack) {
      removedTuple = list.remove(0);
    }
    int index = 0;
    for (Tuple t : list) {
      if (compare(tuple, t) >= 0) {
        break;
      }
      index++;
    }
    list.add(index, tuple);
    return removedTuple;
  }

  private int compare(Tuple tuple1, Tuple tuple2) {
    SimpleData first = ordering == Ordering.ASCENDING ? tuple1.get(position) : tuple2.get(position);
    SimpleData second = ordering == Ordering.ASCENDING ? tuple2.get(position) : tuple1.get(position);
    if (first instanceof TInt) {
      return ((TInt) first).compareTo(second);
    } else if (first instanceof TLong) {
      return ((TLong) first).compareTo(second);
    } else if (first instanceof TString) {
      return ((TString) first).compareTo(second);
    } else if (first instanceof TBoolean) {
      return ((TBoolean) first).compareTo(second);
    } else {
      return first.compareTo(second);
    }
  }

  private int getPositionOfValueFor(Tuple tuple, String attribute) {
    for (int i = 0; i < tuple.getNElements(); i++) {
      String attr = ((TString) tuple.get(i)).getValue();
      if (attr.equals(attribute)) {
        return i + 1;
      }
    }
    return -1;
  }
}
