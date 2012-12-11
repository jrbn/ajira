package arch.utils;

import java.util.HashMap;
import java.util.Map;

public class LocalCounter {

	private Map<String, Long> counters = new HashMap<>();

	public synchronized long getCounter(String name) {
		long n = 0;
		if (counters.containsKey(name))
			n = counters.get(name);
		n++;
		counters.put(name, n);
		return n;

	}
}
