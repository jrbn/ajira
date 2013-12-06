package nl.vu.cs.ajira.examples.aurora.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import nl.vu.cs.ajira.storage.Writable;

/**
 * A filter consists of one or more constraints on the values of a tuple.
 * 
 * @author Alessandro Margara
 */
public class Filter implements Writable, Iterable<Constraint> {
  private Set<Constraint> constraints;

  public Filter() {
    constraints = new HashSet<Constraint>();
  }

  public Filter(Set<Constraint> constraints) {
    this.constraints = constraints;
  }

  public boolean isSatisfiedBy(StreamTuple tuple) {
    for (Constraint constraint : constraints) {
      if (!constraint.isSatisfiedBy(tuple)) {
        return false;
      }
    }
    return true;
  }

  @Override
  public void readFrom(DataInput input) throws IOException {
    constraints = new HashSet<Constraint>();
    int numConstraints = input.readInt();
    for (int i = 0; i < numConstraints; i++) {
      Constraint constraint = new Constraint(input);
      constraints.add(constraint);
    }
  }

  @Override
  public void writeTo(DataOutput output) throws IOException {
    output.writeInt(constraints.size());
    for (Constraint constraint : constraints) {
      constraint.writeTo(output);
    }
  }

  @Override
  public Iterator<Constraint> iterator() {
    return constraints.iterator();
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((constraints == null) ? 0 : constraints.hashCode());
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
    if (!(obj instanceof Filter)) {
      return false;
    }
    Filter other = (Filter) obj;
    if (constraints == null) {
      if (other.constraints != null) {
        return false;
      }
    } else if (!constraints.equals(other.constraints)) {
      return false;
    }
    return true;
  }

}
