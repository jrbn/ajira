package nl.vu.cs.ajira.examples.aurora.actions.io.network.support;

import java.io.Serializable;
import java.util.LinkedHashMap;

import nl.vu.cs.ajira.data.types.SimpleData;
import nl.vu.cs.ajira.data.types.TBoolean;
import nl.vu.cs.ajira.data.types.TInt;
import nl.vu.cs.ajira.data.types.TLong;
import nl.vu.cs.ajira.data.types.TString;
import nl.vu.cs.ajira.data.types.Tuple;

/**
 * This class is used to receive tuples from clients.
 * 
 * @author Alessandro Margara
 */
public class NetworkTuple implements Serializable {
  private static final long serialVersionUID = 4415218392842668216L;
  private final LinkedHashMap<String, NetworkTupleValue> data;

  public NetworkTuple() {
    data = new LinkedHashMap<String, NetworkTupleValue>();
  }

  public void addAttribute(String name, String value) {
    data.put(name, new NetworkTupleValue(value));
  }

  public void addAttribute(String name, int value) {
    data.put(name, new NetworkTupleValue(value));
  }

  public void addAttribute(String name, long value) {
    data.put(name, new NetworkTupleValue(value));
  }

  public void addAttribute(String name, boolean value) {
    data.put(name, new NetworkTupleValue(value));
  }

  public void getTuple(Tuple tuple) {
    SimpleData[] content = new SimpleData[data.size() * 2];
    int pos = 0;
    for (String name : data.keySet()) {
      content[pos++] = new TString(name);
      NetworkTupleValue value = data.get(name);
      switch (value.getType()) {
      case STRING:
        content[pos++] = new TString(value.getStringVal());
        break;
      case INT:
        content[pos++] = new TInt(value.getIntVal());
        break;
      case LONG:
        content[pos++] = new TLong(value.getLongVal());
        break;
      case BOOLEAN:
        content[pos++] = new TBoolean(value.getBooleanVal());
        break;
      }
    }
    tuple.set(content);
  }
}
