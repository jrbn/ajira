package nl.vu.cs.ajira.examples.aurora.api.support;

public class AddChannelOperatorInfo implements OperatorInfo {

  private final int channelId;

  public AddChannelOperatorInfo(int channelId) {
    this.channelId = channelId;
  }

  public int getChannelId() {
    return channelId;
  }

}
