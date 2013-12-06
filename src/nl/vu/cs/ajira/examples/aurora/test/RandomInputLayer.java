package nl.vu.cs.ajira.examples.aurora.test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import nl.vu.cs.ajira.Context;
import nl.vu.cs.ajira.actions.ActionContext;
import nl.vu.cs.ajira.chains.Location;
import nl.vu.cs.ajira.data.types.TInt;
import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.data.types.TupleFactory;
import nl.vu.cs.ajira.datalayer.InputLayer;
import nl.vu.cs.ajira.datalayer.TupleIterator;

public class RandomInputLayer extends InputLayer {
  private static final int numTuples = 100000;
  private Map<Integer, RandomInputLayerIterator> iterators;

  @Override
  protected void load(Context context) throws Exception {
    iterators = new HashMap<Integer, RandomInputLayerIterator>();
  }

  @Override
  public TupleIterator getIterator(Tuple tuple, ActionContext context) {
    int value = ((TInt) tuple.get(0)).getValue();
    if (!iterators.containsKey(value)) {
      generateIteratorFor(value);
    }
    return iterators.get(value);
  }

  @Override
  public void releaseIterator(TupleIterator itr, ActionContext context) {
    // Nothing to do
  }

  @Override
  public Location getLocations(Tuple tuple, ActionContext context) {
    // For now it supports only a local machine.
    return Location.THIS_NODE;
  }

  private void generateIteratorFor(int value) {
    List<Tuple> tuples = new ArrayList<Tuple>();
    for (int i = 0; i < numTuples; i++) {
      TInt content = new TInt(value);
      tuples.add(TupleFactory.newTuple(content));
    }
    iterators.put(value, new RandomInputLayerIterator(tuples));
  }

  class RandomInputLayerIterator extends TupleIterator {
    private final Iterator<Tuple> it;

    public RandomInputLayerIterator(List<Tuple> tuples) {
      it = tuples.iterator();
    }

    @Override
    protected boolean next() throws Exception {
      return it.hasNext();
    }

    @Override
    public void getTuple(Tuple tuple) throws Exception {
      it.next().copyTo(tuple);
    }

    @Override
    public boolean isReady() {
      return true;
    }

  }

}
