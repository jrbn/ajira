package nl.vu.cs.ajira.examples.aurora.api;

public class SplitExecutionPath extends ExecutionPath {
  private int reconnectAfter;

  public void setReconnectAfter(int reconnectAfter) {
    this.reconnectAfter = reconnectAfter;
  }

  public int getReconnectAfter() {
    return reconnectAfter;
  }

}
