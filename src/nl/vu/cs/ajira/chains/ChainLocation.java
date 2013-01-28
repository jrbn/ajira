package nl.vu.cs.ajira.chains;

public class ChainLocation {

	public static final int V_ALL_NODES = -1;
	public static final int V_THIS_NODE = -2;

	int nodeId = -1;

	public ChainLocation(int nodeId) {
		this.nodeId = nodeId;
	}

	public static final ChainLocation THIS_NODE = new ChainLocation(V_THIS_NODE);
	public static final ChainLocation ALL_NODES = new ChainLocation(V_ALL_NODES);

	public int getValue() {
		return nodeId;
	}

}