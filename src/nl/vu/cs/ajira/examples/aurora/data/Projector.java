package nl.vu.cs.ajira.examples.aurora.data;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import nl.vu.cs.ajira.data.types.SimpleData;
import nl.vu.cs.ajira.data.types.TString;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.data.types.TupleFactory;
import nl.vu.cs.ajira.examples.aurora.data.exceptions.AttributeNotFoundException;

/**
 * Simple class implementing the project operator on a tuple.
 * 
 * @author Alessandro Margara
 */
public class Projector {
  private final Set<String> attributesToProject;
  private final List<Integer> attributesPositions;
  private boolean first;
  private final SimpleData[] data;

  public Projector(Set<String> attributesToProject) {
    this.attributesToProject = attributesToProject;
    data = new SimpleData[attributesToProject.size() * 2];
    attributesPositions = new ArrayList<Integer>();
    first = true;
  }

  public Tuple project(Tuple tuple) throws AttributeNotFoundException {
    if (first) {
      determineAttributesPositions(tuple);
      first = false;
    }
    int pos = 0;
    for (Integer i : attributesPositions) {
      data[pos++] = tuple.get(i);
      data[pos++] = tuple.get(i + 1);
    }
    return TupleFactory.newTuple(data);
  }

  private void determineAttributesPositions(Tuple tuple) {
    for (int i = 0; i < tuple.getNElements(); i += 2) {
      String s = ((TString) tuple.get(i)).getValue();
      if (attributesToProject.contains(s)) {
        attributesPositions.add(i);
      }
    }
  }
}
