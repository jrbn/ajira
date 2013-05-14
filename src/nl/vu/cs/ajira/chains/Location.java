package nl.vu.cs.ajira.chains;

public class Location {

	public static final int V_ALL_NODES = -1;
	public static final int V_THIS_NODE = -2;

	int nodeId = -1;

	/**
	 * Custom constructor. Sets the id of the node.
	 * @param nodeId
	 * 		The new value of the nodeId.
	 */
	public Location(int nodeId) {
		this.nodeId = nodeId;
	}

	public static final Location THIS_NODE = new Location(V_THIS_NODE);
	public static final Location ALL_NODES = new Location(V_ALL_NODES);

	/**
	 * 
	 * @return
	 * 		The node id.
	 */
	public int getValue() {
		return nodeId;
	}

}
