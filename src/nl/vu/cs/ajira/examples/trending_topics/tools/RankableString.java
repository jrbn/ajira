package nl.vu.cs.ajira.examples.trending_topics.tools;

import nl.vu.cs.ajira.data.types.TLong;
import nl.vu.cs.ajira.data.types.TString;
import nl.vu.cs.ajira.data.types.Tuple;

public class RankableString implements Comparable<RankableString> {

	private static final String toStringSeparator = "|";

	private final String str;
	private final long count;

	public RankableString(String str, long count) {
		if (str == null) {
			throw new IllegalArgumentException("The object must not be null");
		}
		if (count < 0) {
			throw new IllegalArgumentException("The count must be >= 0");
		}
		this.str = str;
		this.count = count;
	}

	/**
	 * Construct a new instance based on the provided {@link Tuple}.
	 * 
	 * @param tuple
	 * 
	 * @return new instance based on the provided tuple
	 */
	public static RankableString from(Tuple tuple) {
		String str = ((TString) tuple.get(0)).getValue();
		long count = ((TLong) tuple.get(1)).getValue();
		return new RankableString(str, count);
	}

	public String getString() {
		return str;
	}

	public long getCount() {
		return count;
	}

	@Override
	public int compareTo(RankableString other) {
		long delta = this.getCount() - other.getCount();
		if (delta > 0) {
			return 1;
		} else if (delta < 0) {
			return -1;
		} else {
			return 0;
		}
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (count ^ (count >>> 32));
		result = prime * result + ((str == null) ? 0 : str.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (!(obj instanceof RankableString)) {
			return false;
		}
		RankableString other = (RankableString) obj;
		if (count != other.count) {
			return false;
		}
		if (str == null) {
			if (other.str != null) {
				return false;
			}
		} else if (!str.equals(other.str)) {
			return false;
		}
		return true;
	}

	@Override
	public String toString() {
		StringBuffer buf = new StringBuffer();
		buf.append("[");
		buf.append(str);
		buf.append(toStringSeparator);
		buf.append(count);
		buf.append("]");
		return buf.toString();
	}
}
