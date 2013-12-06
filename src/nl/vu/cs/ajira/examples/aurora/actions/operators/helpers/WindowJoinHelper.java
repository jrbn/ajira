package nl.vu.cs.ajira.examples.aurora.actions.operators.helpers;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Queue;
import java.util.TreeMap;

import nl.vu.cs.ajira.data.types.SimpleData;
import nl.vu.cs.ajira.data.types.TInt;
import nl.vu.cs.ajira.data.types.TLong;
import nl.vu.cs.ajira.data.types.TString;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.data.types.TupleFactory;

// FIXME: currently works only for numeric values
public class WindowJoinHelper implements JoinHelper {
  private final String joinAttribute;
  private final int size;
  private final int channelId1;
  private final int channelId2;

  private final Queue<Tuple> queue1 = new LinkedList<Tuple>();
  private final Queue<Tuple> queue2 = new LinkedList<Tuple>();
  private final NavigableMap<Long, List<Tuple>> map1 = new TreeMap<Long, List<Tuple>>();
  private final NavigableMap<Long, List<Tuple>> map2 = new TreeMap<Long, List<Tuple>>();

  private final LinkedHashMap<String, SimpleData> mapForJoin = new LinkedHashMap<String, SimpleData>();

  private boolean first1 = true;
  private boolean first2 = true;
  private int attrPos1;
  private int attrPos2;

  public WindowJoinHelper(String joinAttribute, int size, int channelId1, int channelId2) {
    this.joinAttribute = joinAttribute;
    this.size = size;
    this.channelId1 = channelId1;
    this.channelId2 = channelId2;
  }

  @Override
  public List<Tuple> push(Tuple tuple) {
    int channelId = getChannelId(tuple);
    initSupportVariables(tuple, channelId);

    int attrPosInTuple = (channelId == channelId1) ? attrPos1 : attrPos2;
    Queue<Tuple> queue = (channelId == channelId1) ? queue1 : queue2;
    Map<Long, List<Tuple>> map = (channelId == channelId1) ? map1 : map2;
    Map<Long, List<Tuple>> mapToJoin = (channelId == channelId1) ? map2 : map1;

    SimpleData value = tuple.get(attrPosInTuple);
    long numValue = (value instanceof TInt) ? ((TInt) value).getValue() : ((TLong) value).getValue();

    addTuple(tuple, queue, map, attrPosInTuple, numValue);

    return joinTuples(channelId, tuple, mapToJoin, numValue);
  }

  private int getChannelId(Tuple tuple) {
    return ((TInt) tuple.get(1)).getValue();
  }

  private List<Tuple> joinTuples(int channelId, Tuple tuple, Map<Long, List<Tuple>> map, long value) {
    if (first1 || first2) {
      return null;
    }
    List<Tuple> tuplesToJoin = map.get(value);
    if (tuplesToJoin == null) {
      return null;
    }
    // Perform join and return results
    List<Tuple> results = new ArrayList<Tuple>();
    for (Tuple tupleToJoin : tuplesToJoin) {
      Tuple result = (channelId == channelId1) ? join(tupleToJoin, tuple) : join(tuple, tupleToJoin);
      results.add(result);
    }
    return results;
  }

  private Tuple join(Tuple tuple1, Tuple tuple2) {
    mapForJoin.clear();
    for (int i = 2; i < tuple1.getNElements(); i += 2) {
      String name = ((TString) tuple1.get(i)).getValue();
      mapForJoin.put(name, tuple1.get(i + 1));
    }
    for (int i = 2; i < tuple2.getNElements(); i += 2) {
      String name = ((TString) tuple2.get(i)).getValue();
      mapForJoin.put(name, tuple2.get(i + 1));
    }
    SimpleData[] data = new SimpleData[mapForJoin.size() * 2];
    int pos = 0;
    for (String key : mapForJoin.keySet()) {
      data[pos++] = new TString(key);
      data[pos++] = mapForJoin.get(key);
    }
    return TupleFactory.newTuple(data);
  }

  private void addTuple(Tuple tuple, Queue<Tuple> queue, Map<Long, List<Tuple>> map, int pos, long tupleVal) {
    while (queue.size() >= size) {
      Tuple t = queue.remove();
      SimpleData data = t.get(pos);
      long val = (data instanceof TInt) ? ((TInt) data).getValue() : ((TLong) data).getValue();
      List<Tuple> storedTuple = map.get(val);
      storedTuple.remove(t);
      if (storedTuple.isEmpty()) {
        map.remove(val);
      }
    }
    List<Tuple> list = map.get(tupleVal);
    if (list == null) {
      list = new ArrayList<Tuple>();
      map.put(tupleVal, list);
    }
    list.add(tuple);
    queue.add(tuple);
  }

  private void initSupportVariables(Tuple tuple, int channelId) {
    if (first1 && channelId == channelId1) {
      attrPos1 = getValuePosForAttribute(tuple, joinAttribute);
      first1 = false;
    } else if (first2 && channelId == channelId2) {
      attrPos2 = getValuePosForAttribute(tuple, joinAttribute);
      first2 = false;
    }
  }

  private int getValuePosForAttribute(Tuple tuple, String attribute) {
    for (int i = 0; i < tuple.getNElements(); i += 2) {
      String attrName = ((TString) tuple.get(i)).getValue();
      if (attrName.equals(attribute)) {
        return i + 1;
      }
    }
    return 0;
  }
}
