package nl.vu.cs.ajira.examples.aurora.actions.io.network;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.Socket;

import nl.vu.cs.ajira.data.types.Tuple;
import nl.vu.cs.ajira.datalayer.TupleIterator;
import nl.vu.cs.ajira.examples.aurora.actions.io.network.support.NetworkTuple;

public class NetworkTupleIterator extends TupleIterator {
  private final ObjectInputStream in;

  public NetworkTupleIterator(Socket socket) throws IOException {
    in = new ObjectInputStream(socket.getInputStream());
  }

  @Override
  protected boolean next() throws Exception {
    return true;
  }

  @Override
  public void getTuple(Tuple tuple) throws Exception {
    Object obj = in.readObject();
    assert (obj instanceof NetworkTuple);
    NetworkTuple networkTuple = (NetworkTuple) obj;
    networkTuple.getTuple(tuple);
  }

  @Override
  public boolean isReady() {
    return true;
  }

}
