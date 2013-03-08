package nl.vu.cs.ajira.chains;

public class ChainLocation {

	public static final int V_ALL_NODES = -1;
	public static final int V_THIS_NODE = -2;

	int nodeId = -1;

	/**
	 * Custom constructor. Sets the id of the node.
	 * @param nodeId
	 * 		The new value of the nodeId.
	 */
	public ChainLocation(int nodeId) {
		this.nodeId = nodeId;
	}

	public static final ChainLocation THIS_NODE = new ChainLocation(V_THIS_NODE);
	public static final ChainLocation ALL_NODES = new ChainLocation(V_ALL_NODES);

	/**
	 * 
	 * @return
	 * 		The node id.
	 */
	public int getValue() {
		return nodeId;
	}

}
