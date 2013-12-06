package nl.vu.cs.ajira.examples.aurora.data;

public class Pair<E> {
  private final E firstElement;
  private final E secondElement;

  public Pair(E firstElement, E secondElement) {
    this.firstElement = firstElement;
    this.secondElement = secondElement;
  }

  public E getFirstElement() {
    return firstElement;
  }

  public E getSecondElement() {
    return secondElement;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((firstElement == null) ? 0 : firstElement.hashCode());
    result = prime * result + ((secondElement == null) ? 0 : secondElement.hashCode());
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
    if (!(obj instanceof Pair)) {
      return false;
    }
    @SuppressWarnings("rawtypes")
    Pair other = (Pair) obj;
    if (firstElement == null) {
      if (other.firstElement != null) {
        return false;
      }
    } else if (!firstElement.equals(other.firstElement)) {
      return false;
    }
    if (secondElement == null) {
      if (other.secondElement != null) {
        return false;
      }
    } else if (!secondElement.equals(other.secondElement)) {
      return false;
    }
    return true;
  }

  @Override
  public String toString() {
    return "Pair [" + firstElement + ", " + secondElement + "]";
  }

}
