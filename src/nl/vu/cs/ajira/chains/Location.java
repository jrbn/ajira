package nl.vu.cs.ajira.chains;

/*
 * Represents a range of nodes. Special cases are predefined.
 */
public class Location {

	private static final int V_ALL_NODES = -1;
	private static final int V_THIS_NODE = -2;

	private final int startNode;
	private final int endNode;

	/**
	 * Constructor that specifies a range consisting of a single node.
	 * 
	 * @param nodeId
	 *            The new value of the nodeId.
	 */
	public Location(int nodeId) {
		this.startNode = nodeId;
		this.endNode = nodeId;
	}

	/**
	 * Constructor specifying a range from <code>start</code> to
	 * <code>end</code>.
	 * 
	 * @param start
	 *            start of the range
	 * @param end
	 *            end of the range (included in the range)
	 */
	public Location(int start, int end) {
		this.startNode = start;
		this.endNode = end;
	}

	/** Special case: range indicating the current node. */
	public static final Location THIS_NODE = new Location(V_THIS_NODE);

	/** Special case: range indicating all nodes. */
	public static final Location ALL_NODES = new Location(V_ALL_NODES);

	/**
	 * Returns the start of the range.
	 * 
	 * @return the start
	 */
	public int getStart() {
		return startNode;
	}

	/**
	 * Returns the end of the range.
	 * 
	 * @return the end
	 */
	public int getEnd() {
		return endNode;
	}
}
