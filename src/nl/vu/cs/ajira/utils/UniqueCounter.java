package nl.vu.cs.ajira.utils;

import java.util.HashMap;
import java.util.Map;

public class UniqueCounter {

	private Map<String, Long> counters = new HashMap<String, Long>();

	private final int myNode;
	private final int nNodes;

	public UniqueCounter(int nNodes, int myNode) {
		this.nNodes = nNodes;
		this.myNode = myNode;
	}

	public UniqueCounter() {
		this(1, 0);
	}

	public synchronized long getCounter(String name) {

		long n;

		if (counters.containsKey(name)) {
			n = counters.get(name) + nNodes;
		} else {
			n = myNode;
		}
		counters.put(name, n);
		return n;
	}

	public synchronized void init(String name, long init) {
		long n = ((init + nNodes - 1) / nNodes) * nNodes + myNode;
		counters.put(name, n);
	}
	
	public synchronized void removeCounter(String name) {
		counters.remove(name);
	}
	
	public synchronized boolean hasCounter(String name) {
		return counters.containsKey(name);
	}
}
