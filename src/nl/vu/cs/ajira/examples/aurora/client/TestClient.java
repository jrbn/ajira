package nl.vu.cs.ajira.examples.aurora.client;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import nl.vu.cs.ajira.examples.aurora.actions.io.network.support.NetworkTuple;

public class TestClient {
  private static final int defaultSeed = 0;
  private static final int defaultMaxValue = 1000;

  private final String address;
  private final int port;
  private final List<String> attributes;

  private final Random random;
  private final int maxValue;

  private boolean stop;

  public static void main(String args[]) {
    TestClient client = new TestClient("127.0.0.1", 9000, "A", "B", "C");
    try {
      client.startSendingTuples();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public TestClient(String address, int port, int seed, int maxValue, String... attributes) {
    this.address = address;
    this.port = port;
    this.attributes = new ArrayList<String>();
    for (String attribute : attributes) {
      this.attributes.add(attribute);
    }
    random = new Random(seed);
    this.maxValue = maxValue;
    stop = false;
  }

  public TestClient(String address, int port, String... attributes) {
    this(address, port, defaultSeed, defaultMaxValue, attributes);
  }

  public void startSendingTuples() throws UnknownHostException, IOException {
    Socket socket = new Socket(address, port);
    ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
    while (true) {
      synchronized (this) {
        if (stop) {
          socket.close();
          return;
        }
      }
      out.writeObject(generateTuple());
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  public synchronized void stop() {
    stop = true;
  }

  private NetworkTuple generateTuple() {
    NetworkTuple tuple = new NetworkTuple();
    for (String name : attributes) {
      int val = random.nextInt(maxValue);
      tuple.addAttribute(name, val);
    }
    return tuple;
  }

}
