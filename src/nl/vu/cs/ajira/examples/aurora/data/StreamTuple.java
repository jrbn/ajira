package nl.vu.cs.ajira.examples.aurora.data;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

import nl.vu.cs.ajira.data.types.SimpleData;
import nl.vu.cs.ajira.data.types.TString;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.data.types.TupleFactory;

/**
 * Special kind of Tuple composed of <attribute, value> pairs.
 * 
 * Given the restrictions of Ajira in extending the Tuple class, it is implemented as a wrapper of Tuple, which provides
 * all the methods require to access and manipulate pairs' values.
 * 
 * @author Alessandro Margara
 */
public class StreamTuple {
  private final Tuple tuple;
  private LinkedHashMap<String, SimpleData> attributes;

  public StreamTuple(Tuple tuple) {
    this.tuple = tuple;
  }

  public StreamTuple(SimpleData... data) {
    tuple = TupleFactory.newTuple(data);
  }

  public StreamTuple(LinkedHashMap<String, SimpleData> attributes) {
    int numElements = attributes.size() * 2;
    SimpleData[] data = new SimpleData[numElements];
    int pos = 0;
    for (String name : attributes.keySet()) {
      data[pos++] = new TString(name);
      data[pos++] = attributes.get(name);
    }
    tuple = TupleFactory.newTuple(data);
    this.attributes = attributes;
  }

  public Tuple getTuple() {
    return tuple;
  }

  /**
   * Return the list of attributes' names (in the order in which they appear in the tuple)
   */
  public List<String> getAttributes() {
    List<String> result = new ArrayList<String>();
    for (int i = 0; i < tuple.getNElements(); i += 2) {
      String elem = ((TString) tuple.get(i)).getValue();
      result.add(elem);
    }
    return result;
  }

  /**
   * Return the position of the value of attribute in the tuple, or -1 if the attribute is not found.
   */
  public int getPositionOfValueFor(String attribute) {
    for (int i = 0; i < tuple.getNElements(); i += 2) {
      String elem = ((TString) tuple.get(i)).getValue();
      if (elem.equals(attribute)) {
        return i + 1;
      }
    }
    return -1;
  }

  /**
   * Return the value for the give attribute.
   */
  public SimpleData getValueFor(String attribute) {
    initAttributesMap();
    return attributes.get(attribute);
  }

  /**
   * Return the value in the given position.
   */
  public SimpleData getValueIn(int pos) {
    return tuple.get(pos);
  }

  private void initAttributesMap() {
    if (attributes != null) {
      return;
    }
    attributes = new LinkedHashMap<String, SimpleData>();
    for (int i = 0; i < tuple.getNElements(); i += 2) {
      String name = ((TString) tuple.get(i)).getValue();
      SimpleData value = tuple.get(i + 1);
      attributes.put(name, value);
    }
  }

}