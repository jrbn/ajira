package arch.utils;

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
}
