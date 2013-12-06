package nl.vu.cs.ajira.examples.aurora.actions.operators.helpers;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import nl.vu.cs.ajira.data.types.SimpleData;
import nl.vu.cs.ajira.data.types.TInt;
import nl.vu.cs.ajira.data.types.TLong;
import nl.vu.cs.ajira.data.types.TString;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.data.types.TupleFactory;
import nl.vu.cs.ajira.examples.aurora.data.AggregationFunction;
import nl.vu.cs.ajira.examples.aurora.data.StreamTuple;

public class AggregateHelper {
  private enum AttributeType {
    STRING, NUMERIC, UNIQUE
  }

  private final Map<String, StatQueue> stringQueues;
  private final Map<Long, StatQueue> numericQueues;
  private final StatQueue uniqueQueue;

  private final AggregationFunction function;
  private final int size;
  private final int advance;

  private final List<Integer> attributesPositions;
  private final SimpleData[] template;

  private int aggregatePosition;
  private int groupByPosition;
  private final AttributeType groupByType;

  public AggregateHelper(StreamTuple streamTuple, String aggregateAttribute, AggregationFunction function, int size, int advance, Set<String> attributesToPreserve, String groupBy) {
    this.function = function;
    this.size = size;
    this.advance = advance;
    stringQueues = new HashMap<String, StatQueue>();
    numericQueues = new HashMap<Long, StatQueue>();
    attributesPositions = new ArrayList<Integer>();
    template = new SimpleData[attributesToPreserve.size() * 2];

    // Make sure that the partitioning attribute is preserved
    if (groupBy != null && !attributesToPreserve.contains(groupBy)) {
      attributesToPreserve.add(groupBy);
    }

    // Determine the position of attributes to preserve and aggregates
    determineAttributesPositions(streamTuple, attributesToPreserve, aggregateAttribute, groupBy);
    uniqueQueue = new StatQueue(aggregatePosition, function);

    // Determining whether a GroupBy clause is defined
    if (groupBy == null) {
      groupByType = AttributeType.UNIQUE;
    } else {
      SimpleData value = streamTuple.getValueFor(groupBy);
      // TODO: assuming only string or numeric values
      if (value instanceof TString) {
        groupByType = AttributeType.STRING;
      } else {
        groupByType = AttributeType.NUMERIC;
      }
    }
  }

  public Tuple push(StreamTuple tuple) {
    switch (groupByType) {
    case UNIQUE:
      return push(tuple, uniqueQueue);
    case NUMERIC:
      long numValue = getNumericValueInPosition(tuple, groupByPosition);
      if (!numericQueues.containsKey(numValue)) {
        numericQueues.put(numValue, new StatQueue(aggregatePosition, function));
      }
      return push(tuple, numericQueues.get(numValue));
    case STRING:
      String stringValue = getStringValueInPosition(tuple, groupByPosition);
      if (!stringQueues.containsKey(stringValue)) {
        stringQueues.put(stringValue, new StatQueue(aggregatePosition, function));
      }
      return push(tuple, stringQueues.get(stringValue));
    }
    return null;
  }

  private long getNumericValueInPosition(StreamTuple tuple, int pos) {
    SimpleData data = tuple.getTuple().get(pos + 1);
    if (data instanceof TInt) {
      return ((TInt) data).getValue();
    } else {
      return ((TLong) data).getValue();
    }
  }

  private String getStringValueInPosition(StreamTuple tuple, int pos) {
    return ((TString) tuple.getValueIn(pos + 1)).getValue();
  }

  private Tuple push(StreamTuple tuple, StatQueue queue) {
    queue.push(tuple);
    Tuple result = generateOutput(queue);
    queue.advance(size, advance);
    return result;
  }

  private Tuple generateOutput(StatQueue queue) {
    if (queue.isEmpty()) {
      return null;
    }
    StreamTuple tupleToCopy = queue.peek();
    int pos = 0;
    Tuple t = tupleToCopy.getTuple();
    template[pos++] = t.get(aggregatePosition);
    template[pos++] = queue.getAggrValue();
    for (Integer i : attributesPositions) {
      template[pos++] = t.get(i);
      template[pos++] = t.get(i + 1);
    }
    return TupleFactory.newTuple(template);
  }

  private void determineAttributesPositions(StreamTuple tuple, Set<String> attributesToPreserve, String aggregateAttribute, String groupByAttribute) {
    Tuple t = tuple.getTuple();
    for (int i = 0; i < t.getNElements(); i += 2) {
      String s = ((TString) t.get(i)).getValue();
      if (s.equals(aggregateAttribute)) {
        aggregatePosition = i;
      } else if (attributesToPreserve.contains(s)) {
        attributesPositions.add(i);
      }
      if (groupByAttribute != null && s.equals(groupByAttribute)) {
        groupByPosition = i;
      }
    }
  }

  private class StatQueue {
    private long min;
    private long max;
    private long sum;
    private final LinkedList<StreamTuple> tuples;
    private final int attrPos;
    private final AggregationFunction function;

    StatQueue(int attrPos, AggregationFunction function) {
      this.attrPos = attrPos;
      this.function = function;
      tuples = new LinkedList<StreamTuple>();
      min = Integer.MAX_VALUE;
      max = Integer.MIN_VALUE;
      sum = 0;
    }

    void push(StreamTuple tuple) {
      tuples.push(tuple);
      long value = getValueForAggr(tuple);
      sum += value;
      if (value < min) {
        min = value;
      }
      if (value > max) {
        max = value;
      }
    }

    boolean isEmpty() {
      return tuples.isEmpty();
    }

    StreamTuple peek() {
      return tuples.peek();
    }

    void pop() {
      StreamTuple tuple = tuples.pop();
      long value = getValueForAggr(tuple);
      sum -= value;
      if (value <= min) {
        recomputeMin();
      }
      if (value >= max) {
        recomputeMax();
      }
    }

    void advance(int size, int advance) {
      if (tuples.size() < size) {
        return;
      }
      for (int i = 0; i < advance; i++) {
        if (tuples.isEmpty()) {
          return;
        }
        pop();
      }
    }

    TLong getAggrValue() {
      switch (function) {
      case MIN:
        return new TLong(min);
      case MAX:
        return new TLong(max);
      case COUNT:
        return new TLong(tuples.size());
      case AVG:
        return tuples.isEmpty() ? new TLong(0) : new TLong(sum / tuples.size());
      case SUM:
        return new TLong(sum);
      default:
        return new TLong(0);
      }
    }

    private long getValueForAggr(StreamTuple tuple) {
      SimpleData data = tuple.getTuple().get(attrPos + 1);
      if (data instanceof TInt) {
        return ((TInt) data).getValue();
      } else {
        return ((TLong) data).getValue();
      }
    }

    private void recomputeMax() {
      max = Integer.MIN_VALUE;
      for (StreamTuple tuple : tuples) {
        long value = getValueForAggr(tuple);
        if (value > max) {
          max = value;
        }
      }
    }

    private void recomputeMin() {
      min = Integer.MAX_VALUE;
      for (StreamTuple tuple : tuples) {
        long value = getValueForAggr(tuple);
        if (value < min) {
          min = value;
        }
      }
    }

  }

}
