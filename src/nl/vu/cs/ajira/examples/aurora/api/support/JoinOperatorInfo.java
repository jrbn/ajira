package nl.vu.cs.ajira.examples.aurora.api.support;

public class JoinOperatorInfo implements OperatorInfo {
  private final String attributeName;
  private final int size;
  private final int channelId1;
  private final int channelId2;
  private final boolean winJoin;

  public JoinOperatorInfo(String attributeName, int size, int channelId1, int channelId2, boolean winJoin) {
    super();
    this.attributeName = attributeName;
    this.size = size;
    this.channelId1 = channelId1;
    this.channelId2 = channelId2;
    this.winJoin = winJoin;
  }

  public String getAttributeName() {
    return attributeName;
  }

  public int getSize() {
    return size;
  }

  public int getChannelId1() {
    return channelId1;
  }

  public int getChannelId2() {
    return channelId2;
  }

  public boolean getWinJoin() {
    return winJoin;
  }

}
